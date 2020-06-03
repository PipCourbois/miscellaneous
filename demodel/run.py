
import deact
import argparse
from datetime import datetime
from pyspark.sql import SparkSession


# Parse arguments
def date_str(x):
	try:
		datetime.strftime(datetime.strptime(x, '%Y-%m-%d'), '%Y-%m-%d')
	except Exception:
		raise argparse.ArgumentTypeError('Argument must be a string formatted as "yyyy-mm-dd"')
	return x


def percent(x):
	try:
		x = float(x)
	except Exception:
		raise argparse.ArgumentTypeError('Argument must be a float in the range [0.0, 1.0]')
	if x < 0.0 or x > 1.0:
		raise argparse.ArgumentTypeError('Argument must be in the range [0.0, 1.0]')
	return x


parser = argparse.ArgumentParser()
parser.add_argument('--train_date', type=date_str, default='2018-11-01',
	help='String of format "yyyy-mm-dd" representing the feature date for the model\'s training data')
parser.add_argument('--score_date', type=date_str, default=None,
	help='String of format "yyyy-mm-dd" representing the feature date for the active customers being scored by the model')
parser.add_argument('--train_pct', type=percent, default=0.10,
	help='Float between 0.0 and 1.0 giving the proportion of the training data to use when training the model')
parser.add_argument('--calibrate_pct', type=percent, default=0.0,
	help='Float between 0.0 and 1.0 giving the proportion of the training data to use when calibrating the model')
parser.add_argument('--validate_pct', type=percent, default=0.0,
	help='Float between 0.0 and 1.0 giving the proportion of the training data to use when validating the model')
parser.add_argument('--calibrate_model', action='store_true', default=False,
	help='Makes model predictions on calibration data and fits an isotonic regression to adjust predictions')
parser.add_argument('--validate_model', action='store_true', default=False,
	help='Makes model predictions on validation data and evaluates the predictions')
parser.add_argument('--score_active_users', action='store_true', default=False,
	help='Makes model predictions on active customers')
parser.add_argument('--mask_prob', action='store_true', default=False,
	help='Drops the predicted deact probabilities for customers with large recency values and assigns probability = 1.0')
parser.add_argument('--mask_days', type=int, default=0,
	help='Integer giving the number of days to apply the probability mask.' +
		'E.g. a value of 3 masks customers who will deact in the next 3 days.')
args = parser.parse_args()

train_date = args.train_date
score_date = args.score_date
train_pct = args.train_pct
calibrate_pct = args.calibrate_pct
validate_pct = args.validate_pct
calibrate_model = args.calibrate_model
validate_model = args.validate_model
score_active_users = args.score_active_users
mask_prob = args.mask_prob
mask_days = args.mask_days


# Validate arguments
if train_pct + calibrate_pct + validate_pct > 1.0:
	raise parser.error('Argument Validation Error: train_pct + calibrate_pct + validate_pct cannot exceed 1.0')

if calibrate_model and calibrate_pct == 0.0:
	raise parser.error('Argument Validation Error: calibrate_pct > 0.0 is required when calibrate_model is True')

if validate_model and validate_pct == 0.0:
	raise parser.error('Argument Validation Error: validate_pct > 0.0 is required when validate_model is True')

if score_active_users and score_date is None:
	raise parser.error('Argument Validation Error: score_date is required when score_active_users is True')

if not calibrate_model and calibrate_pct > 0.0:
	print('[ {0} ] : Overwriting calibrate_pct to 0.0% since calibrate_model is False'.format(datetime.utcnow()))
	calibrate_pct = 0.

if not validate_model and validate_pct > 0.0:
	print('[ {0} ] : Overwriting validate_pct to 0.0% since validate_model is False'.format(datetime.utcnow()))
	validate_model = 0.

if not score_active_users and score_date is not None:
	print('[ {0} ] : score_date will be ignored since score_active_users is False'.format(datetime.utcnow()))
score_date = score_date if score_active_users else 'N/A'

# Coalesce some variables
not_used_pct = 1 - train_pct - calibrate_pct - validate_pct

# Start SparkSession
# warehouse_location = 'hdfs://cerebro-namenode-vip.snc1/user/grp_gdoop_clv/grp_gdoop_clv_hiveDB.db'
# python_location = './ANACONDA/anaconda2_env/bin/python'
# archives_location = 'hdfs:////user/grp_gdoop_admin/anaconda/anaconda2_env.zip#ANACONDA'

# spark = SparkSession\
# 	.builder\
# 	.master('yarn')\
# 	.appName('deact-model')\
# 	.config('spark.submit.deployMode','client')\
# 	.config('spark.executor.instances', 50)\
# 	.config('spark.yarn.appMasterEnv.PYSPARK_PYTHON',python_location)\
# 	.config('spark.yarn.dist.archives',archives_location)\
# 	.config('spark.sql.warehouse.dir',warehouse_location)\
# 	.config('spark.yarn.queue','public')\
# 	.enableHiveSupport()\
# 	.getOrCreate()

spark = SparkSession\
	.builder\
	.enableHiveSupport()\
	.getOrCreate()

spark.sparkContext.setLogLevel('WARN')


########################
# DEACT MODEL WORKFLOW #
########################

print('''
[ {0} ] : BEGINNING DEACT MODEL PIPELINE

	PARAMETERS:
	Training date = {1}
	Proportion of training data used to:
		Train model = {2:0.0%}
		Calibrate model = {3:0.0%}
		Validate model = {4:0.0%}
		Not used = {5:0.0%}
	Scoring date = {6}
	Masking deact probabilities for {7:d} days
'''.format(datetime.utcnow(), train_date, train_pct, calibrate_pct, validate_pct, not_used_pct, score_date, mask_days))

# Save locations
hdfs_loc = 'hdfs://cerebro-namenode-vip.snc1/user/grp_gdoop_clv/deact-model/'
eval_path = '/home/svc_clv/Consumer-Intelligence/Models/Deact-Model/model_evaluation/'

# Run data pipeline
pl = deact.Pipeline(spark, train_date, score_date, train_pct, calibrate_pct, validate_pct)
dfs = pl.run(calibrate_model, validate_model, score_active_users)

# Train model
model = deact.Model(train_date, score_date, pl.feature_list)
model.train(dfs['training'])

# Calibrate model probabilities
if calibrate_model:
	model.calibrate(dfs['calibration'])

# Make predictions on validation data, evaluate them, and plot results
if validate_model:
	model.predict(dfs['validation'], 'validation', calibrate_model, mask_prob, False, mask_days)
	evaluator = deact.Evaluator(model.pred_df, calibrate_model, mask_prob, True, eval_path)
	evaluator.class_metrics()
	evaluator.calibration()
	evaluator.decision_boundary_curves()
	evaluator.recency_aggregation()
	evaluator.feature_importances(model.feat_imp, min_imp=0.001)

# Make predictions on scoring data and save them. Check that predictions are stable over time.
pct_change = None
if score_active_users:
	model.predict(dfs['scoring'], 'scoring', calibrate_model, mask_prob, True, mask_days, hdfs_loc)
	model.total_deacts(calibrate_model, 'scoring', True, hdfs_loc)
	pct_change = model.compare_to_prev_day(spark, calibrate_model, hdfs_loc, max_change=0.05)

print('\n[ {0} ] : COMPLETED DEACT MODEL PIPELINE\n'.format(datetime.utcnow()))
