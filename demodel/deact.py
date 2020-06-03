import re
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from pyspark.sql.types import DoubleType, IntegerType
import pyspark.sql.functions as F
from pyspark.ml import Pipeline as MLPipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.regression import IsotonicRegression

plt.switch_backend('agg')


class Pipeline:

    def __init__(self, spark, train_date, score_date, train_pct, calibrate_pct=0., validate_pct=0.):
        assert (0. <= train_pct <= 1.)
        assert (0. <= calibrate_pct <= 1.)
        assert (0. <= validate_pct <= 1.)
        assert (train_pct + calibrate_pct + validate_pct <= 1.)
        print('\nDATA PIPELINE\n')

        self.spark = spark
        self.train_date = train_date
        self.score_date = score_date
        self.train_pct = train_pct
        self.calibrate_pct = calibrate_pct
        self.validate_pct = validate_pct

    def _load_feature_df(self, feature_date, training):
        phase = 'training' if training else 'scoring'
        print('[ {0} ] : Loading {1} feature data'.format(datetime.utcnow(), phase))

        table_suffix = 't365d' if training else 'scoring'
        features_df = self.spark.sql('from grp_gdoop_clv_db.keep_cdf_final_features_' + table_suffix) \
            .filter(F.col('record_date') == feature_date) \
            .drop('record_date', 'zip_code_cat_x')

        if training:
            target_df = self.spark.sql('select * from grp_gdoop_clv_db.ce_keep_deact_target') \
                .filter(F.col('record_date') == feature_date) \
                .select('consumer_id', 'deactivated')

            final = features_df \
                .join(target_df, features_df.consumer_id == target_df.consumer_id, how='left') \
                .drop(target_df.consumer_id)

            return final

        else:
            return features_df

    def _train_calibrate_validate_split(self, features_df):
        print('[ {0} ] : Splitting training data into model training, calibration, and validation data'
            .format(datetime.utcnow()))
        splits = features_df.randomSplit([self.train_pct, self.calibrate_pct, self.validate_pct,
                                          1 - self.train_pct - self.calibrate_pct - self.validate_pct])
        return splits[0], splits[1], splits[2]

    def _make_feature_list(self, all_cols, cat_cols, indexers):
        features = list(filter(lambda x: x.endswith('_x') and not x.endswith('_cat_x'), all_cols))
        for i, col in enumerate(cat_cols):
            for label in indexers[i].labels:
                features.extend([col + '_' + re.sub('\W+', '_', str(label).strip())])
        self.feature_list = features

    def _one_hot_encode_pl(self, train_raw):
        print('[ {0} ] : Creating feature engineering pipeline'.format(datetime.utcnow()))
        all_cols = train_raw.columns
        cat_cols = list(filter(lambda x: x.endswith('_cat_x'), all_cols))

        indexers = [StringIndexer(inputCol=c, outputCol=c.replace('_cat_x', '_index'),
                                  handleInvalid='keep') for c in cat_cols]
        one_hots = [OneHotEncoderEstimator(inputCols=[c.replace('_cat_x', '_index')],
                                           outputCols=[c.replace('_cat_x', '_vec_x')], handleInvalid='keep',
                                           dropLast=False) for c in cat_cols]

        self.one_hot_plm = MLPipeline(stages=indexers + one_hots).fit(train_raw)
        self._make_feature_list(all_cols, cat_cols, self.one_hot_plm.stages[:len(cat_cols)])

    def _assemble_features(self, raw_df, data_type):
        print('[ {0} ] : Feature engineering {1} data'.format(datetime.utcnow(), data_type))
        cat_cols = list(filter(lambda x: x.endswith('_cat_x'), raw_df.columns))
        df = self.one_hot_plm.transform(raw_df) \
            .drop(*cat_cols)

        features = list(filter(lambda x: x.endswith('_x'), df.columns))
        assembler = VectorAssembler(inputCols=features, outputCol='features', handleInvalid='keep')
        return assembler.transform(df)

    def _training_data(self, calibrate_model, validate_model):
        train_features_df = self._load_feature_df(self.train_date, True)
        train_raw, calibrate_raw, validate_raw = self._train_calibrate_validate_split(train_features_df)
        train_raw.cache()

        # Create one-hot encoding pipeline that will be applied to all DFs
        self._one_hot_encode_pl(train_raw)

        train_df = self._assemble_features(train_raw, 'model training').cache()
        train_raw.unpersist()

        if calibrate_model:
            calibrate_raw.cache()
            calibrate_df = self._assemble_features(calibrate_raw, 'model calibration').cache()
            calibrate_raw.unpersist()
        else:
            calibrate_df = None

        if validate_model:
            validate_raw.cache()
            validate_df = self._assemble_features(validate_raw, 'model validation').cache()
            validate_raw.unpersist()
        else:
            validate_df = None

        return train_df, calibrate_df, validate_df

    def _scoring_data(self):
        score_features_df = self._load_feature_df(self.score_date, False).cache()
        score_df = self._assemble_features(score_features_df, 'scoring').cache()
        score_features_df.unpersist()
        return score_df

    def run(self, calibrate_model, validate_model, score_active_users):
        train_df, calibrate_df, validate_df = self._training_data(calibrate_model, validate_model)
        if score_active_users:
            score_df = self._scoring_data()
        else:
            score_df = None
        return {'training': train_df, 'calibration': calibrate_df, 'validation': validate_df, 'scoring': score_df}

    def __repr__(self):
        return '<Pipeline(train_date={0}, score_date={1})>'.format(self.train_date, self.score_date)

    def __str__(self):
        return '<Pipeline(train_date={0}, score_date={1})>'.format(self.train_date, self.score_date)


class Model:

    def __init__(self, train_date, score_date, feature_list):
        print('\nMODELING\n')
        self.train_date = train_date
        self.score_date = score_date
        self.feature_list = feature_list
        self.classifier = GBTClassifier(
            labelCol='deactivated',
            featuresCol='features',
            maxDepth=6,
            minInstancesPerNode=500,
            subsamplingRate=0.5,
        )

        # Variables to be defined later
        self.fitted_model = None
        self.feat_imp = None
        self.ir_model = None
        self.pred_df = None
        self.total_deact_df = None

    def train(self, train_df):
        print('[ {0} ] : Training model'.format(datetime.utcnow()))
        model = self.classifier.fit(train_df)
        self.fitted_model = model
        self._feat_imp()

    def _feat_imp(self):
        print('[ {0} ] : Zipping model feature importances'.format(datetime.utcnow()))
        importances = self.fitted_model.featureImportances
        self.feat_imp = sorted(zip(self.feature_list, importances), key=lambda x: -x[1])

    def calibrate(self, df_to_calibrate):
        # Make initial prediction on calibration data set
        self.predict(df_to_calibrate, 'calibration', False, False, False)
        print('[ {0} ] : Calibrating model'.format(datetime.utcnow()))

        # Convert initial probability to input feature
        pred_df_cal = VectorAssembler(inputCols=['prob_deact'], outputCol='prob_feature', handleInvalid='keep') \
            .transform(self.pred_df)

        # Fit calibration function on results
        ir = IsotonicRegression(
            labelCol='deactivated',
            predictionCol='prob_deact_cal',
            featuresCol='prob_feature'
        )
        ir_model = ir.fit(pred_df_cal)
        self.ir_model = ir_model

    def _save_predictions(self, with_calibration, mask_prob, data_type, hdfs_loc):
        print('[ {0} ] : Saving {1} predictions to cerebro'.format(datetime.utcnow(), data_type))
        date = self.score_date if self.score_date is not None else self.train_date
        filepath = hdfs_loc + 'ce_keep_deact_predictions/record_date=' + date
        self.filepath = filepath

        base_cols = ['consumer_id', 'recency_x']
        prob_col = ['masked_prob'] if mask_prob else ['prob_deact_cal'] if with_calibration else ['prob_deact']
        target_col = ['deactivated'] if (data_type == 'calibration' or data_type == 'validation') else []
        cols = base_cols + prob_col + target_col
        results = self.pred_df.select(*cols) \
            .withColumnRenamed(prob_col[0], 'prob_deact')

        results.write \
            .mode('overwrite') \
            .format('orc') \
            .option('orc.compress', 'SNAPPY') \
            .save(filepath)
        print('[ {0} ] : Deact probabilities stored in {1}'.format(datetime.utcnow(), filepath))

    def predict(self, df_to_predict, data_type, with_calibration, mask_prob, save_results, mask_days=3, hdfs_loc=None):
        print('[ {0} ] : Making predictions on {1} data'.format(datetime.utcnow(), data_type))
        # Make prediction and extract deact probabilities
        prob_deact = F.udf(lambda x: x.toArray().tolist()[1], DoubleType())
        pred_df = self.fitted_model \
            .transform(df_to_predict) \
            .withColumn('prob_deact', prob_deact(F.col('probability'))) \
            .drop('rawPrediction', 'probability', 'prediction')

        if with_calibration:
            pred_df_cal = VectorAssembler(inputCols=['prob_deact'], outputCol='prob_feature', handleInvalid='keep') \
                .transform(pred_df)
            self.pred_df = self.ir_model \
                .transform(pred_df_cal) \
                .drop('prob_feature') \
                .cache()
        else:
            self.pred_df = pred_df.cache()

        if mask_prob:
            prob_col = 'prob_deact_cal' if with_calibration else 'prob_deact'
            self.pred_df = self.pred_df \
                .withColumn('masked_prob', F.when(F.col('recency_x') >= (365-mask_days), F.lit(1.))
                    .otherwise(F.col(prob_col))) \
                .cache()

        if save_results:
            self._save_predictions(with_calibration, mask_prob, data_type, hdfs_loc)

    def _save_total_deacts(self, data_type, hdfs_loc):
        print('[ {0} ] : Saving total deactivations for {1} data to cerebro'.format(datetime.utcnow(), data_type))
        date = self.score_date if self.score_date is not None else self.train_date
        filepath = hdfs_loc + 'ce_keep_deact_totals/record_date=' + date
        self.total_deact_df.write \
            .mode('overwrite') \
            .format('orc') \
            .option('orc.compress', 'SNAPPY') \
            .save(filepath)
        print('[ {0} ] : Deact probabilities stored in {1}'.format(datetime.utcnow(), filepath))

    def total_deacts(self, with_calibration, data_type, save_results, hdfs_loc):
        print('[ {0} ] : Calculating total deactivations by day for {1} data'.format(datetime.utcnow(), data_type))
        date = self.score_date if self.score_date is not None else self.train_date
        prob_col = 'prob_deact_cal' if with_calibration else 'prob_deact'

        def deact_date(date_, recency):
            return datetime.strftime(datetime.strptime(date_, '%Y-%m-%d') + timedelta(days=365 - recency), '%Y-%m-%d')

        deact_date_udf = F.udf(deact_date)

        results = self.pred_df \
            .filter((F.col('recency_x') >= 0) & (F.col('recency_x') <= 365)) \
            .withColumn('deact_date', deact_date_udf(F.lit(date), F.col('recency_x'))) \
            .select('deact_date', prob_col) \
            .groupBy('deact_date') \
            .agg(F.sum(prob_col).cast(IntegerType()).alias('total_deacts')) \
            .sort('deact_date') \
            .cache()

        self.total_deact_df = results

        if save_results:
            self._save_total_deacts(data_type, hdfs_loc)

    def compare_to_prev_day(self, spark, calibrate_model, hdfs_loc, max_change):
        print('[ {0} ] : Comparing predicted deact rate to previous day'.format(datetime.utcnow()))
        prob_col = 'prob_deact_cal' if calibrate_model else 'prob_deact'
        pred_pct_deact = self.pred_df \
            .agg(
                F.sum(prob_col).alias('deacts'),
                F.count('consumer_id').alias('users'),
                ) \
            .withColumn('pct_deact', F.col('deacts') / F.col('users')) \
            .select('pct_deact') \
            .collect()[0][0]

        date = self.score_date if self.score_date is not None else self.train_date
        yesterday = datetime.strftime(datetime.strptime(date, '%Y-%m-%d') + timedelta(days=-1), '%Y-%m-%d')
        prev_pred_pct_deact = spark.read.orc(hdfs_loc + 'ce_keep_deact_predictions/record_date=' + yesterday) \
            .agg(
                F.sum('prob_deact').alias('deacts'),
                F.count('consumer_id').alias('users'),
                ) \
            .withColumn('pct_deact', F.col('deacts') / F.col('users')) \
            .select('pct_deact') \
            .collect()[0][0]

        pct_change = abs((pred_pct_deact / prev_pred_pct_deact) - 1.0)

        if pct_change < max_change:
            print('[ {0} ] : The predicted deactivation rate changed {1:0.2%} from the previous day'
                .format(datetime.utcnow(), pct_change))
        else:
            raise ValueError('''The change in the predicted deactivation rate is {0:0.2%}, which exceeds the maximum allowable 
                change of {1:0.2%}. Please check upstream data sources for any issues.'''
                .format(pct_change, max_change))
        return pct_change

    def __repr__(self):
        return '<Model(model_type={0}, train_date={1})>'.format(self.classifier.__class__, self.train_date)

    def __str__(self):
        return '<Model(model_type={0}, train_date={1})>'.format(self.classifier.__class__, self.train_date)


class Evaluator:

    def __init__(self, prediction_df, calibrated, masked, save_results, eval_path):
        print('\nEVALUATING MODEL\n')
        self.prediction_df = prediction_df
        self.calibrated = calibrated
        self.masked = masked
        self.classes = [0, 1]
        self.save_results = save_results
        self.eval_path = eval_path
        self.colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
            '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

        # Values defined in eval methods
        self.confusion_df = None
        self.class_metrics_df = None
        self.calibration_df = None
        self.curve_df = None
        self.recency_df = None
        self.feat_imp_df = None

    def class_metrics(self):
        # Get dictionary of class counts
        classes = [0, 1]
        counts = self._confusion_matrix(classes)
        prob_col = 'masked_prob' if self.masked else 'prob_deact_cal' if self.calibrated else 'prob_deact'

        print('[ {0} ] : Calculating model performance by class'.format(datetime.utcnow()))
        eval_dict = {}
        for c in classes:
            eval_dict[c] = {}
            eval_dict[c]['N'] = sum(counts[c][all_] for all_ in counts[c])
            try:
                eval_dict[c]['precision'] = counts[c][c] / float(sum(counts[all_][c] for all_ in counts))
            except ZeroDivisionError:
                eval_dict[c]['precision'] = 0.0
            try:
                eval_dict[c]['recall'] = counts[c][c] / float(eval_dict[c]['N'])
                eval_dict[c]['predOverActualN'] = sum(counts[all_][c] for all_ in counts[c]) / float(eval_dict[c]['N'])
            except ZeroDivisionError:
                eval_dict[c]['recall'] = 0.0
                eval_dict[c]['predOverActualN'] = 0.0
            try:
                eval_dict[c]['f1'] = (2 * eval_dict[c]['precision'] * eval_dict[c]['recall']) / \
                                     (eval_dict[c]['precision'] + eval_dict[c]['recall'])
            except ZeroDivisionError:
                eval_dict[c]['f1'] = 0.0

            eval_dict[c]['avgLogLoss'] = self.prediction_df \
                .filter(F.col('deactivated') == c) \
                .withColumn('log_loss', -F.log((F.lit(c)*F.col(prob_col)) + (F.lit(1-c)*(F.lit(1.)-F.col(prob_col))))) \
                .agg(F.avg('log_loss').alias('avg_log_loss')) \
                .collect()[0][0]

        # Weighted average
        eval_dict['weighted'] = {}
        eval_dict['weighted']['N'] = sum([eval_dict[c]['N'] for c in classes])
        metrics = ['precision', 'recall', 'f1', 'avgLogLoss', 'predOverActualN']
        for m in metrics:
            eval_dict['weighted'][m] = sum(eval_dict[c]['N'] * eval_dict[c][m] for c in classes) / float(
                eval_dict['weighted']['N'])

        # noinspection PyTypeChecker
        eval_df = pd.DataFrame.from_dict(eval_dict, orient='index')[
            ['N', 'precision', 'recall', 'f1', 'avgLogLoss', 'predOverActualN']] \
            .reindex(classes + ['weighted'])
        self.class_metrics_df = eval_df

        # Save results
        if self.save_results:
            eval_df.to_csv(self.eval_path + 'class_metrics.tsv', sep='\t', index_label='class')

    def _confusion_matrix(self, classes):
        print('[ {0} ] : Calculating confusion matrix for model predictions'.format(datetime.utcnow()))
        prob_col = 'masked_prob' if self.masked else 'prob_deact_cal' if self.calibrated else 'prob_deact'
        counts = {}
        for actual in classes:
            counts[actual] = {}
            for predicted in classes:
                counts[actual][predicted] = self.prediction_df.filter((F.col('deactivated') == actual) &
                                                                    (F.round(F.col(prob_col), 0) == predicted)).count()

        # N = sum([counts[k1][k2] for k1 in counts.keys() for k2 in counts[k1].keys()])
        # accuracy = sum([counts[k][k] for k in counts.keys()]) / float(N)

        count_df = pd.DataFrame.from_dict(counts, orient='index')[classes] \
            .rename(columns={x: 'predicted_' + str(x) for x in classes},
                    index={x: 'actual_' + str(x) for x in classes})
        count_df['total'] = sum([count_df['predicted_' + str(c)] for c in classes])
        count_df = count_df.append(
            pd.DataFrame({col: count_df[col].sum() for col in count_df.columns}, index=['total']))
        self.confusion_df = count_df

        if self.save_results:
            count_df.to_csv(self.eval_path + 'prediction_matrix.tsv', sep='\t', index_label='')

        return counts

    def calibration(self):
        print('[ {0} ] : Calculating probability calibration'.format(datetime.utcnow()))
        prob_col = 'masked_prob' if self.masked else 'prob_deact_cal' if self.calibrated else 'prob_deact'
        df = self.prediction_df \
            .withColumn('prob_bucket', F.round(F.col(prob_col), 2)) \
            .groupBy('prob_bucket') \
            .agg(F.avg('deactivated').alias('pct_deactivated')) \
            .sort('prob_bucket') \
            .toPandas()
        self.calibration_df = df

        if self.save_results:
            df.to_csv(self.eval_path + 'probability_calibration.tsv', sep='\t', index=False)

        self._calibration_plot()

    def _calibration_plot(self):
        print('[ {0} ] : Plotting calibration curve'.format(datetime.utcnow()))
        df = self.calibration_df
        fig = plt.figure(figsize=(8, 8))
        plt.plot(df.prob_bucket, df.pct_deactivated, marker='', color=self.colors[0], label='Model')
        plt.plot([0, 1], [0, 1], marker='', color=self.colors[1], linestyle='--', label='Ideal')
        plt.title('Calibration (Reliability) Curve', fontsize='x-large')
        plt.xlabel('Assigned Probability')
        plt.ylabel('Fraction of Deactivations')
        plt.legend(loc='best', fontsize='large')
        self.plot_calibration = fig

        if self.save_results:
            plt.savefig(self.eval_path + 'plot_calibration.png')

    def decision_boundary_curves(self):
        print('[ {0} ] : Calculating points on PR and ROC curves'.format(datetime.utcnow()))
        d = {'cutoff': [], 'tp': [], 'fp': [], 'tpr': [], 'fpr': [], 'precision': []}
        pos = self.prediction_df.filter(self.prediction_df.deactivated == 1).cache()
        neg = self.prediction_df.filter(self.prediction_df.deactivated == 0).cache()

        p = pos.count()
        n = neg.count()
        prob_col = 'masked_prob' if self.masked else 'prob_deact_cal' if self.calibrated else 'prob_deact'

        for cutoff in range(101):
            tp = pos.filter(F.col(prob_col) >= (cutoff / 100.)).count()
            fp = neg.filter(F.col(prob_col) >= (cutoff / 100.)).count()

            d['cutoff'].append(cutoff / 100.)
            d['tp'].append(tp)
            d['fp'].append(fp)
            d['tpr'].append(tp / float(p))  # Same as recall
            d['fpr'].append(fp / float(n))
            if tp + fp > 0:
                d['precision'].append(tp / float(tp + fp))
            else:
                d['precision'].append(1.)

        curve_df = pd.DataFrame.from_dict(d)[['cutoff', 'tp', 'fp', 'tpr', 'fpr', 'precision']] \
            .sort_values(['cutoff']) \
            .drop_duplicates(subset=['tp', 'fp'], keep='first') \
            .reset_index(drop=True)
        self.curve_df = curve_df

        if self.save_results:
            curve_df.to_csv(self.eval_path + 'probability_cutoff_curves.tsv', sep='\t', index=False)

        # Calculate areas under curves
        self._areas()

        deact_ratio = p / float(p+n)
        self._curve_plots(deact_ratio)

    def _areas(self):
        print('[ {0} ] : Calculating areas under PR and ROC curves'.format(datetime.utcnow()))
        area_under_pr = 0.
        area_under_roc = 0.
        for i in range(len(self.curve_df)):
            if i > 0:
                # PR
                w = self.curve_df.tpr[i - 1] - self.curve_df.tpr[i]
                h_bar = self.curve_df.precision[i]
                h_tri = self.curve_df.precision[i - 1] - h_bar
                area_under_pr = area_under_pr + (w * h_bar) + (0.5 * w * h_tri)

                # ROC
                w = self.curve_df.fpr[i - 1] - self.curve_df.fpr[i]
                h_bar = self.curve_df.tpr[i]
                h_tri = self.curve_df.tpr[i - 1] - h_bar
                area_under_roc = area_under_roc + (w * h_bar) + (0.5 * w * h_tri)

        self.area_under_pr = area_under_pr
        self.area_under_roc = area_under_roc

    def _curve_plots(self, deact_ratio):
        print('[ {0} ] : Plotting PR and ROC curves'.format(datetime.utcnow()))
        plots = [
            {'name': 'pr', 'x': 'tpr', 'y': 'precision',
             'title': 'Precision-Recall Curve', 'xlabel': 'Recall', 'ylabel': 'Precision',
             'random_line_y': [deact_ratio, deact_ratio], 'random_area': deact_ratio},
            {'name': 'roc', 'x': 'fpr', 'y': 'tpr',
             'title': 'ROC Curve', 'xlabel': 'False Positive Rate', 'ylabel': 'True Positive Rate',
             'random_line_y': [0, 1], 'random_area': 0.5}
        ]

        for p in plots:
            fig = plt.figure(figsize=(8, 8))
            area = getattr(self, 'area_under_' + p['name'])
            plt.plot(self.curve_df[p['x']], self.curve_df[p['y']], marker='', color=self.colors[0],
                     label='model (AUC = {0:0.4f})'.format(area))
            plt.plot([0, 1], p['random_line_y'], marker='', color=self.colors[1], linestyle='--',
                     label='random (AUC = {0:0.4f})'.format(p['random_area']))
            plt.title(p['title'], fontsize='x-large')
            plt.xlabel(p['xlabel'])
            plt.xlim(0, 1)
            plt.ylabel(p['ylabel'])
            plt.ylim(0, 1)
            plt.legend(loc='best', fontsize='large')
            setattr(self, 'plot_' + p['name'], fig)

            if self.save_results:
                plt.savefig('{}plot_{}.png'.format(self.eval_path, p['name']))

    def recency_aggregation(self):
        print('[ {0} ] : Calculating aggregations by recency for model predictions'.format(datetime.utcnow()))
        prob_col = 'masked_prob' if self.masked else 'prob_deact_cal' if self.calibrated else 'prob_deact'
        df = self.prediction_df \
            .filter((F.col('recency_x') >= 0) & (F.col('recency_x') <= 365)) \
            .withColumn('days_until_deact', (F.lit(365) - F.col('recency_x')).cast(IntegerType())) \
            .withColumn('log_loss', F.when(F.col('deactivated') == 1, -F.log(F.col(prob_col)))
                        .otherwise(-F.log(F.lit(1.0) - F.col(prob_col)))) \
            .groupBy('days_until_deact') \
            .agg(F.count('consumer_id').alias('count_users'),
                 F.sum('deactivated').alias('deacts_actual'),
                 F.sum(prob_col).cast(IntegerType()).alias('deacts_pred'),
                 F.avg('log_loss').alias('avg_log_loss'),
                 F.avg(prob_col).alias('avg_prob_deact'),
                 F.avg((F.col('deactivated') == F.round(F.col(prob_col), 0)).cast(IntegerType())).alias('accuracy'),
                 ) \
            .withColumn('pct_deact_actual', F.col('deacts_actual') / F.col('count_users')) \
            .withColumn('pct_deact_pred', F.col('deacts_pred') / F.col('count_users')) \
            .withColumn('pred_over_actual_deacts', F.col('deacts_pred') / F.col('deacts_actual')) \
            .withColumn('diff_pct_deact', F.col('pct_deact_pred') - F.col('pct_deact_actual')) \
            .sort('days_until_deact') \
            .toPandas()
        self.recency_df = df

        if self.save_results:
            df.to_csv(self.eval_path + 'recency_aggregation.tsv', sep='\t', index=False)

        self._recency_plots()

    def _recency_plots(self):
        print('[ {0} ] : Plotting recency aggregations'.format(datetime.utcnow()))
        df = self.recency_df
        plots = [
            {'y': 'count_users', 'title': 'Number of Consumers', 'label': None, 'baseline': False},
            {'y': 'avg_log_loss', 'title': 'Average Log Loss', 'label': 'Model',
             'baseline': {'y': [0.] * len(df), 'label': 'Ideal', 'linestyle': '--'}},
            {'y': 'avg_prob_deact', 'title': 'Average Probability of Deactivation', 'label': 'Model',
             'baseline': {'y': df.pct_deact_actual, 'label': 'Actual', 'linestyle': '-'}},
            # {'y': 'pct_deact_pred', 'title': 'Proportion of Deactivations', 'label': 'Predicted',
            # 	'baseline': {'y': df.pct_deact_actual, 'label': 'Actual', 'linestyle': '-'}},
            {'y': 'diff_pct_deact', 'title': '(% Predicted - % Actual) Deactivations', 'label': 'Model',
             'baseline': {'y': [0.] * len(df), 'label': 'Ideal', 'linestyle': '--'}},
            {'y': 'accuracy', 'title': 'Accuracy', 'label': 'Model',
             'baseline': {'y': [1.] * len(df), 'label': 'Ideal', 'linestyle': '--'}},
        ]

        for p in plots:
            fig = plt.figure(figsize=(8, 8))
            plt.plot(df.days_until_deact, df[p['y']], marker='', color=self.colors[0], label=p['label'])
            if p['baseline']:
                plt.plot(df.days_until_deact, p['baseline']['y'], marker='', color=self.colors[1],
                         linestyle=p['baseline']['linestyle'], label=p['baseline']['label'])
                plt.legend(loc='best', fontsize='large')
            plt.title(p['title'], fontsize='x-large')
            plt.xlabel('Days Until Deactivation (365 - Recency)')
            plt.xlim(0, 365)
            setattr(self, 'plot_' + p['y'], fig)

            if self.save_results:
                plt.savefig('{}plot_{}.png'.format(self.eval_path, p['y']))

    def feature_importances(self, feat_imp_tuples, min_imp):
        print('[ {0} ] : Getting feature importances'.format(datetime.utcnow()))
        # Convert to dataframe
        df = pd.DataFrame(feat_imp_tuples, columns=['feature', 'importance'])
        self.feat_imp_df = df

        # Save results
        if self.save_results:
            df.to_csv(self.eval_path + 'feature_importance.tsv', sep='\t', index=False)

        self._feat_imp_plot(min_imp)

    def _feat_imp_plot(self, min_imp):
        print('[ {0} ] : Plotting feature importances'.format(datetime.utcnow()))
        feat_imp = self.feat_imp_df[self.feat_imp_df.importance >= min_imp]
        n = len(feat_imp)
        fig = plt.figure(figsize=(12, 4))
        plt.bar(range(n), feat_imp.importance, color=self.colors[0])
        plt.xticks(range(n), feat_imp.feature, rotation='vertical')
        plt.title('{0} Features with Importance of at Least {1:0.1%}'.format(n, min_imp))
        self.plot_feat_imp = fig

        if self.save_results:
            plt.savefig(self.eval_path + 'plot_feat_imp.png', bbox_inches='tight')

    def __repr__(self):
        if self.save_results:
            return '<Evaluator(saving_results_to: {0})>'.format(self.eval_path)
        else:
            return '<Evaluator(not_saving_results)>'

    def __str__(self):
        if self.save_results:
            return '<Evaluator(saving_results_to: {0})>'.format(self.eval_path)
        else:
            return '<Evaluator(not_saving_results)>'
