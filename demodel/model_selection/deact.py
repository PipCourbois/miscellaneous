
import re
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from pyspark.sql.types import DoubleType, IntegerType
import pyspark.sql.functions as F
from pyspark.ml import Pipeline as MLPipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier

plt.switch_backend('agg')


class Pipeline:

    def __init__(self, spark, train_date, score_date, train_pct, validate_pct=0.):
        assert (0. <= train_pct <= 1.)
        assert (0. <= validate_pct <= 1.)
        assert (train_pct + validate_pct <= 1.)

        self.spark = spark
        self.train_date = train_date
        self.score_date = score_date
        self.train_pct = train_pct
        self.validate_pct = validate_pct

    def _load_feature_df(self, feature_date, training):
        phase = 'training' if training else 'scoring'
        print('[ ' + str(datetime.utcnow()) + ' ] : Loading ' + phase + ' feature data')

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

    def _train_validate_split(self, features_df):
        print('[ ' + str(datetime.utcnow()) + ' ] : Splitting training data into model training and validation data')
        splits = features_df.randomSplit([self.train_pct, self.validate_pct, 1 - self.train_pct - self.validate_pct])
        return splits[0], splits[1]

    def _make_feature_list(self, all_cols, cat_cols, indexers):
        features = list(filter(lambda x: x.endswith('_x') and not x.endswith('_cat_x'), all_cols))
        for i, col in enumerate(cat_cols):
            for label in indexers[i].labels:
                features.extend([col + '_' + re.sub('\W+', '_', str(label).strip())])
        self.feature_list = features

    def _one_hot_encode_pl(self, train_raw):
        print('[ ' + str(datetime.utcnow()) + ' ] : Creating feature engineering pipeline')
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
        print('[ ' + str(datetime.utcnow()) + ' ] : Feature engineering ' + data_type + ' data')
        cat_cols = list(filter(lambda x: x.endswith('_cat_x'), raw_df.columns))
        df = self.one_hot_plm.transform(raw_df) \
            .drop(*cat_cols)

        features = list(filter(lambda x: x.endswith('_x'), df.columns))
        assembler = VectorAssembler(inputCols=features, outputCol='features', handleInvalid='keep')
        return assembler.transform(df)

    def _training_data(self, validate_model):
        train_features_df = self._load_feature_df(self.train_date, True)
        train_raw, validate_raw = self._train_validate_split(train_features_df)
        train_raw.cache()

        # Create one-hot encoding pipeline that will be applied to all DFs
        self._one_hot_encode_pl(train_raw)

        train_df = self._assemble_features(train_raw, 'model training').cache()
        train_raw.unpersist()

        if validate_model:
            validate_raw.cache()
            validate_df = self._assemble_features(validate_raw, 'model validation').cache()
            validate_raw.unpersist()
        else:
            validate_df = None

        return train_df, validate_df

    def _scoring_data(self):
        score_features_df = self._load_feature_df(self.score_date, False).cache()
        score_df = self._assemble_features(score_features_df, 'scoring').cache()
        score_features_df.unpersist()
        return score_df

    def run(self, validate_model, score_active_users):
        print('\nDATA PIPELINE\n')
        train_df, validate_df = self._training_data(validate_model)
        if score_active_users:
            score_df = self._scoring_data()
        else:
            score_df = None
        return {'training': train_df, 'validation': validate_df, 'scoring': score_df, 'features': self.feature_list}

    def __repr__(self):
        return '<Pipeline(train_date={0}, score_date={1})>'.format(self.train_date, self.score_date)

    def __str__(self):
        return '<Pipeline(train_date={0}, score_date={1})>'.format(self.train_date, self.score_date)


class Model:

    def __init__(self, train_date, score_date, name, classifier, feature_list):
        print('\nMODELING\n')
        self.train_date = train_date
        self.score_date = score_date
        self.name = name
        self.classifier = classifier
        self.feature_list = feature_list

        # Variables to be defined later
        self.pred_df = None
        self.fitted_model = None

    def train(self, train_df):
        print('[ ' + str(datetime.utcnow()) + ' ] : Training model = '+self.name)
        model = MLPipeline(stages=[self.classifier]).fit(train_df)
        self.fitted_model = model

    def _save_predictions(self, data_type, hdfs_loc):
        print('[ ' + str(datetime.utcnow()) + ' ] : Saving '+data_type+' predictions to cerebro for model = '+self.name)
        filepath = hdfs_loc + 'ce_keep_deact_predictions1/record_date=' + self.train_date + '/model=' + self.name
        self.filepath = filepath
        self.pred_df.select('consumer_id', 'recency_x', 'deactivated', 'prob_deact', 'prediction') \
            .write \
            .mode('overwrite') \
            .format('orc') \
            .option('orc.compress', 'SNAPPY') \
            .save(filepath)
        print('[ ' + str(datetime.utcnow()) + ' ] : Deact probabilities stored in ' + filepath)

    def predict(self, df_to_predict, data_type, save_results, hdfs_loc=None):
        print('\nPREDICTING DEACTIVATIONS\n')
        print('[ ' + str(datetime.utcnow()) + ' ] : Making predictions on '+data_type+' data for model = ' + self.name)
        # Make prediction and extract deact probabilities
        prob_deact = F.udf(lambda x: x.toArray().tolist()[1], DoubleType())
        pred_df = self.fitted_model \
            .transform(df_to_predict) \
            .withColumn('prob_deact', prob_deact(F.col('probability'))) \
            .drop('rawPrediction', 'probability') \
            .cache()
        self.pred_df = pred_df

        if save_results:
            self._save_predictions(data_type, hdfs_loc)

    def _counts(self, classes, save_results, eval_path):
        print('[ ' + str(datetime.utcnow()) + ' ] : Calculating count matrix for predictions from model = ' + self.name)
        counts = {}
        for actual in classes:
            counts[actual] = {}
            for predicted in classes:
                counts[actual][predicted] = self.pred_df.filter((F.col('deactivated') == actual) &
                                                                (F.col('prediction') == predicted)).count()
        self.counts = counts
        self.N = sum([counts[k1][k2] for k1 in counts.keys() for k2 in counts[k1].keys()])
        self.accuracy = sum([counts[k][k] for k in counts.keys()]) / float(self.N)

        count_df = pd.DataFrame.from_dict(counts, orient='index')[classes] \
            .rename(columns={x: 'predicted_' + str(x) for x in classes},
                    index={x: 'actual_' + str(x) for x in classes})
        count_df['total'] = sum([count_df['predicted_' + str(c)] for c in classes])
        count_df = count_df.append(
            pd.DataFrame({col: count_df[col].sum() for col in count_df.columns}, index=['total']))
        self.count_df = count_df

        if save_results:
            count_df.to_csv(eval_path + 'model=' + self.name + '/prediction_matrix.tsv', sep='\t', index_label='')

    def _class_metrics(self, classes, save_results, eval_path):
        print('[ ' + str(datetime.utcnow()) + ' ] : Calculating performance by class for model = ' + self.name)
        # Get dictionary of class counts
        counts = self.counts

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

        # Weighted average
        eval_dict['weighted'] = {}
        eval_dict['weighted']['N'] = sum([eval_dict[c]['N'] for c in classes])
        metrics = ['precision', 'recall', 'f1', 'predOverActualN']
        for m in metrics:
            eval_dict['weighted'][m] = sum(eval_dict[c]['N'] * eval_dict[c][m] for c in classes) / float(
                eval_dict['weighted']['N'])

        eval_df = pd.DataFrame.from_dict(eval_dict, orient='index')[
            ['N', 'precision', 'recall', 'f1', 'predOverActualN']] \
            .reindex(classes + ['weighted'])
        self.eval_df = eval_df

        # Save results
        if save_results:
            eval_df.to_csv(eval_path + 'model=' + self.name + '/class_metrics.tsv', sep='\t', index_label='class')

    def _recency_aggregation(self, save_results, eval_path):
        print('[ '+str(datetime.utcnow())+' ] : Calculating recency aggregations for predictions by model = '+self.name)
        df = self.pred_df \
            .filter((F.col('recency_x') >= 0) & (F.col('recency_x') <= 365)) \
            .withColumn('days_until_deact', (F.lit(365) - F.col('recency_x')).cast(IntegerType())) \
            .withColumn('log_loss', F.when(F.col('deactivated') == 1, -F.log(F.col('prob_deact')))
                        .otherwise(-F.log(F.lit(1.0) - F.col('prob_deact')))) \
            .groupBy('days_until_deact') \
            .agg(F.count('consumer_id').alias('count_users'),
                 F.sum('deactivated').alias('deacts_actual'),
                 F.sum('prob_deact').cast(IntegerType()).alias('deacts_pred'),
                 F.avg('log_loss').alias('avg_log_loss'),
                 F.avg('prob_deact').alias('avg_prob_deact'),
                 # Accuracy uses "prediction" column, which assigns consumers to class based on cutoff point of 0.50
                 F.avg((F.col('deactivated') == F.col('prediction')).cast(IntegerType())).alias('accuracy')) \
            .withColumn('pct_deact_actual', F.col('deacts_actual') / F.col('count_users')) \
            .withColumn('pct_deact_pred', F.col('deacts_pred') / F.col('count_users')) \
            .withColumn('pred_over_actual_deacts', F.col('deacts_pred') / F.col('deacts_actual')) \
            .withColumn('diff_pct_deact', F.col('pct_deact_pred') - F.col('pct_deact_actual')) \
            .sort('days_until_deact') \
            .toPandas()
        self.recency_agg = df

        if save_results:
            df.to_csv(eval_path + 'model=' + self.name + '/recency_aggregation.tsv', sep='\t', index=False)

    def _calibration(self, save_results, eval_path):
        print('[ ' + str(datetime.utcnow()) + ' ] : Calculating probability calibration for model = ' + self.name)
        df = self.pred_df \
            .withColumn('prob_bucket', F.round(F.col('prob_deact'), 2)) \
            .groupBy('prob_bucket') \
            .agg(F.avg('deactivated').alias('pct_deactivated')) \
            .sort('prob_bucket') \
            .toPandas()
        self.calibration_df = df

        if save_results:
            df.to_csv(eval_path + 'model=' + self.name + '/probability_calibration.tsv', sep='\t', index=False)

    def _feature_importances(self, save_results, eval_path):
        print('[ ' + str(datetime.utcnow()) + ' ] : Calculating feature importances for model = ' + self.name)

        # Zip features to importances
        features = self.feature_list
        importances = self.fitted_model.stages[-1].featureImportances
        final = sorted(zip(features, importances), key=lambda x: -x[1])
        df = pd.DataFrame(final, columns=['feature', 'importance'])
        self.feat_imp = df

        # Save results
        if save_results:
            df.to_csv(eval_path + 'model='+self.name+'/feature_importance.tsv', sep='\t', index=False)

    def _areas(self):
        print('[ ' + str(datetime.utcnow()) + ' ] : Calculating areas under PR and ROC curves for model = ' + self.name)
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

    def _curves(self, save_results, eval_path):
        print('[ ' + str(datetime.utcnow()) + ' ] : Calculating points on PR and ROC curves for model = ' + self.name)
        d = {'cutoff': [], 'tp': [], 'fp': [], 'tpr': [], 'fpr': [], 'precision': []}
        pos = self.pred_df.filter(self.pred_df.deactivated == 1).cache()
        neg = self.pred_df.filter(self.pred_df.deactivated == 0).cache()

        p = pos.count()
        n = neg.count()
        self.deactivations = p

        for cutoff in range(101):
            tp = pos.filter(pos.prob_deact >= (cutoff / 100.)).count()
            fp = neg.filter(neg.prob_deact >= (cutoff / 100.)).count()

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

        if save_results:
            curve_df.to_csv(eval_path + 'model='+self.name+'/probability_cutoff_curves.tsv', sep='\t', index=False)

        # Calculate areas under curves
        self._areas()

    def evaluate(self, save_results, eval_path=''):
        print('\nEVALUATING MODEL\n')
        classes = [0, 1]

        # Evaluation functions
        self._counts(classes, save_results, eval_path)
        self._class_metrics(classes, save_results, eval_path)
        self._recency_aggregation(save_results, eval_path)
        self._calibration(save_results, eval_path)
        self._feature_importances(save_results, eval_path)
        self._curves(save_results, eval_path)

    def __repr__(self):
        return '<Model(name={0}, train_date={1})>'.format(self.name, self.train_date)

    def __str__(self):
        return '<Model(name={0}, train_date={1})>'.format(self.name, self.train_date)


class Plotting:

    def __init__(self, model_collection, save_results, eval_path):
        print('\nPLOTTING MODEL RESULTS\n')
        self.model_collection = model_collection
        self.num_models = len(model_collection)
        self.save_results = save_results
        self.eval_path = eval_path
        self.colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
            '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    def _curve_plots(self):
        print('[ ' + str(datetime.utcnow()) + ' ] : Plotting PR and ROC curves')
        deact_ratio = self.model_collection[0].deactivations / float(self.model_collection[0].N)
        plots = [
            {'name': 'pr', 'x': 'tpr', 'y': 'precision',
            'title': 'Precision-Recall Curve', 'xlabel': 'Recall', 'ylabel': 'Precision',
            'random_line_y': [deact_ratio, deact_ratio], 'random_area': str(round(deact_ratio, 4))},
            {'name': 'roc', 'x': 'fpr', 'y': 'tpr',
            'title': 'ROC Curve', 'xlabel': 'False Positive Rate', 'ylabel': 'True Positive Rate',
            'random_line_y': [0, 1], 'random_area': '0.5000'}
        ]

        for p in plots:
            fig = plt.figure(figsize=(8, 8))
            for i, m in enumerate(self.model_collection):
                area = getattr(m, 'area_under_'+p['name'])
                plt.plot(m.curve_df[p['x']], m.curve_df[p['y']], marker='', color=self.colors[i],
                    label=m.name+' (AUC = ' + str(round(area, 4)) + ')')
            plt.plot([0, 1], p['random_line_y'], marker='', color=self.colors[self.num_models], linestyle='--',
                     label='random (AUC = ' + p['random_area'] + ')')
            plt.title(p['title'], fontsize='x-large')
            plt.xlabel(p['xlabel'])
            plt.xlim(0, 1)
            plt.ylabel(p['ylabel'])
            plt.ylim(0, 1)
            plt.legend(loc='best', fontsize='large')
            setattr(self, 'plot_'+p['name'], fig)

            if self.save_results:
                plt.savefig('{}plot_{}.png'.format(self.eval_path, p['name']))

    def _recency_plots(self):
        print('[ ' + str(datetime.utcnow()) + ' ] : Plotting recency aggregations')
        df1 = self.model_collection[0].recency_agg
        plots = [
            {'y': 'count_users', 'title': 'Number of Consumers', 'label': None, 'baseline': False},
            {'y': 'avg_log_loss', 'title': 'Average Log Loss', 'label': 'Model',
             'baseline': {'y': [0.] * len(df1), 'label': 'ideal', 'linestyle': '--'}},
            {'y': 'avg_prob_deact', 'title': 'Average Probability of Deactivation', 'label': 'Model',
             'baseline': {'y': df1.pct_deact_actual, 'label': 'actual', 'linestyle': '-'}},
            # {'y': 'pct_deact_pred', 'title': 'Proportion of Deactivations', 'label': 'Predicted',
            #  'baseline': {'y': df1.pct_deact_actual, 'label': 'actual', 'linestyle': '-'}},
            {'y': 'diff_pct_deact', 'title': '(% Predicted - % Actual) Deactivations', 'label': 'Model',
             'baseline': {'y': [0.] * len(df1), 'label': 'ideal', 'linestyle': '--'}},
            {'y': 'accuracy', 'title': 'Accuracy', 'label': 'Model',
             'baseline': {'y': [1.] * len(df1), 'label': 'ideal', 'linestyle': '--'}},
        ]

        for p in plots:
            fig = plt.figure(figsize=(8, 8))
            for i, m in enumerate(self.model_collection):
                plt.plot(m.recency_agg.days_until_deact, m.recency_agg[p['y']], marker='', color=self.colors[i],
                    label=m.name)
            if p['baseline']:
                plt.plot(df1.days_until_deact, p['baseline']['y'], marker='', color=self.colors[self.num_models],
                    linestyle=p['baseline']['linestyle'], label=p['baseline']['label'])
                plt.legend(loc='best', fontsize='large')
            plt.title(p['title'], fontsize='x-large')
            plt.xlabel('Days Until Deactivation (365 - Recency)')
            plt.xlim(0, 365)
            setattr(self, 'plot_'+p['y'], fig)

            if self.save_results:
                plt.savefig('{}plot_{}.png'.format(self.eval_path, p['y']))

    def _calibration_plot(self):
        print('[ ' + str(datetime.utcnow()) + ' ] : Plotting calibration curve')
        fig = plt.figure(figsize=(8, 8))
        for i, m in enumerate(self.model_collection):
            plt.plot(m.calibration_df.prob_bucket, m.calibration_df.pct_deactivated, marker='', color=self.colors[i],
                label=m.name)
        plt.plot([0, 1], [0, 1], marker='', color=self.colors[self.num_models], linestyle='--', label='ideal')
        plt.title('Calibration (Reliability) Curve', fontsize='x-large')
        plt.xlabel('Probability Bucket')
        plt.ylabel('Fraction of Deactivations')
        plt.legend(loc='best', fontsize='large')
        self.plot_calibration = fig

        if self.save_results:
            plt.savefig(self.eval_path + 'plot_calibration.png')

    def _feat_imp_plot(self, min_imp):
        print('[ ' + str(datetime.utcnow()) + ' ] : Plotting feature importances')
        fig = plt.figure(figsize=(12, 20))
        for i, m in enumerate(self.model_collection):
            feat_imp = m.feat_imp[m.feat_imp.importance >= min_imp]
            n = len(feat_imp)
            plt.subplot(3, 1, i+1)
            plt.bar(range(n), feat_imp.importance, color=self.colors[0])
            plt.xticks(range(n), feat_imp.feature, rotation='vertical')
            plt.title('{0} Features with Importance of at Least {1:.1%} for Model = {2}'.format(n, min_imp, m.name))
        plt.tight_layout()
        self.plot_feat_imp = fig

        if self.save_results:
            plt.savefig(self.eval_path + 'plot_feat_imp.png', bbox_inches='tight')

    def create(self):
        self._curve_plots()
        self._recency_plots()
        self._calibration_plot()
        self._feat_imp_plot(min_imp=0.001)

    def __repr__(self):
        return '<Plotter(num_models={0}, model_collection=[{1}])>'.format(self.num_models,
                                                                    ', '.join([str(x) for x in self.model_collection]))

    def __str__(self):
        return '<Plotter(num_models={0})>'.format(self.num_models)


def workflow(spark, train_date, score_date, train_pct, validate_pct, validate_model, score_active_users, save_results):
    # Print workflow information
    print('\n[ ' + str(datetime.utcnow()) + ' ] : Beginning DEACT MODEL pipeline with parameters:\n')
    print('\tTraining date = ' + train_date)
    print('\tProportion of training data used to:')
    print('\t\tTrain model = ' + str(int(train_pct * 100)) + '%')

    if validate_model:
        print('\t\tValidate model = ' + str(int(validate_pct * 100)) + '%')
        print('\t\tNot used = ' + str(int((1 - train_pct - validate_pct) * 100)) + '%')
    else:
        print('\t\tValidate model = 0%')
        print('\t\tNot used = ' + str(int((1 - train_pct) * 100)) + '%')

    if score_active_users:
        print('\tScoring date = ' + score_date)
    else:
        print('\tNot making predictions on scoring data')

    # Save locations
    hdfs_loc = 'hdfs://cerebro-namenode-vip.snc1/user/grp_gdoop_clv/deact-model/'
    eval_path = '/home/ceasterwood/Consumer-Intelligence/Models/Deact-Model/model_selection/model_evaluation/'

    # Run data pipeline
    pl = Pipeline(spark, train_date, score_date, train_pct, validate_pct)
    dfs = pl.run(validate_model, score_active_users)

    # Model dict
    params = {'labelCol': 'deactivated', 'featuresCol': 'features',
              'maxDepth': 6, 'minInstancesPerNode': 500, 'subsamplingRate': 0.5,
              'numTrees': 1000, 'maxIter': 20}
    models = [
        {
            'name': 'random_forest',
            'train_df': dfs['training'],
            'features': dfs['features'],
            'classifier': RandomForestClassifier(
                labelCol=params['labelCol'],
                featuresCol=params['featuresCol'],
                maxDepth=params['maxDepth'],
                minInstancesPerNode=params['minInstancesPerNode'],
                subsamplingRate=params['subsamplingRate'],
                numTrees=params['numTrees'],
            ),
            'validate_df': dfs['validation'],
        },
        {
            'name': 'gradient_boosted_trees',
            'train_df': dfs['training'],
            'features': dfs['features'],
            'classifier': GBTClassifier(
                labelCol=params['labelCol'],
                featuresCol=params['featuresCol'],
                maxDepth=params['maxDepth'],
                minInstancesPerNode=params['minInstancesPerNode'],
                subsamplingRate=params['subsamplingRate'],
                maxIter=params['maxIter'],
            ),
            'validate_df': dfs['validation'],
        },
        {
            'name': 'gbt_1_feature',
            'train_df': VectorAssembler(inputCols=['recency_x'], outputCol='recency_feature', handleInvalid='keep')
                .transform(dfs['training']),
            'features': ['recency_x'],
            'classifier': GBTClassifier(
                labelCol=params['labelCol'],
                featuresCol='recency_feature',
                maxDepth=params['maxDepth'],
                minInstancesPerNode=params['minInstancesPerNode'],
                subsamplingRate=params['subsamplingRate'],
                maxIter=params['maxIter'],
            ),
            'validate_df': VectorAssembler(inputCols=['recency_x'], outputCol='recency_feature', handleInvalid='keep')
                .transform(dfs['validation']),
        },
    ]

    # Train models, make predictions on validation data, and evaluate predictions
    model_collection = []
    for m in models:
        model = Model(train_date, score_date, m['name'], m['classifier'], m['features'])
        model.train(m['train_df'])
        model.predict(m['validate_df'], 'validation', save_results, hdfs_loc)
        model.evaluate(save_results, eval_path)
        model_collection.append(model)

    # Create plots comparing results from all models
    plots = Plotting(model_collection, save_results, eval_path)
    plots.create()

    print('\n[ ' + str(datetime.utcnow()) + ' ] : Completed DEACT MODEL pipeline\n')
    return model_collection
