
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor

from dsp_airflow.spark_submit import SSHSparkSubmitOperator
from dsp_airflow.hiveserver2_hive_operator import HiveServer2HiveOperator


default_args = {
	'owner': 'ceasterwood',
	'depends_on_past': False,
	'start_date': datetime(2019, 10, 1),
	'email': ['ceasterwood@groupon.com'],
	'email_on_failure': True,
	'email_on_retry': True,
	'retries': 1,
}

#############
# LOCATIONS #
#############

# Local
DEACT_DIR = '/home/svc_clv/Consumer-Intelligence/Models/Deact-Model'

# HDFS
# HDFS_DIR = 'hdfs://cerebro-namenode-vip.snc1/user/grp_gdoop_clv/deact-model/'
HIVE_DB = 'grp_gdoop_clv_db'

# Spark
PYTHON_DIR = './ANACONDA/anaconda2_env/bin/python'
ARCHIVES_DIR = 'hdfs:////user/grp_gdoop_admin/anaconda/anaconda2_env.zip#ANACONDA'
WAREHOUSE_DIR = 'hdfs://cerebro-namenode-vip.snc1/user/grp_gdoop_clv/grp_gdoop_clv_hiveDB.db'

TARGET_DATA_DAYS = 3


###################
# MODEL VARIABLES #
###################

def date_add(date_str, days):
	return datetime.strftime(datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=days), '%Y-%m-%d')


today = datetime.strftime(datetime.utcnow(), '%Y-%m-%d')
train_date = date_add(today, -366)
score_date = date_add(today, -1)
train_pct = 0.10
calibrate_pct = 0.10
validate_pct = 0.10
calibrate_model = True
validate_model = False
score_active_users = True
mask_prob = True
mask_days = 3


#######
# DAG #
#######

# Task Generators

def verify_partition_task(table_suffix, record_date):
	task = NamedHivePartitionSensor(
		task_id='verify_{0}_partition'.format(table_suffix),
		metastore_conn_id='svc_push_ds_hive_cerebro_metastore',
		partition_names=['{0}.keep_cdf_final_features_{1}/record_date={2}'.format(HIVE_DB, table_suffix, record_date)],
		timeout=60*60*6  # Wait 6 hours to see if partition arrives
	)
	return task


def add_partition_task(table, date):
	task = HiveServer2HiveOperator(
		task_id='add_{0}_partition'.format(table),
		hqls='alter table {0}.ce_keep_deact_{1} add if not exists partition(record_date = "{2}")'
			.format(HIVE_DB, table, date),
		hiveserver2_conn_id='hive_cerebro_prod',
		schema=HIVE_DB,
	)
	return task


def drop_partition_task(table, date):
	task = HiveServer2HiveOperator(
		task_id='drop_old_{0}_partition'.format(table),
		hqls='alter table {0}.ce_keep_deact_{1} drop if exists partition(record_date <= "{2}")'
			.format(HIVE_DB, table, date),
		hiveserver2_conn_id='hive_cerebro_prod',
		schema=HIVE_DB,
		)
	return task


def partition_stats_task(table, date):
	task = HiveServer2HiveOperator(
		task_id='{0}_partition_stats'.format(table),
		hqls='analyze table {0}.ce_keep_deact_{1} partition(record_date = "{2}") compute statistics'
			.format(HIVE_DB, table, date),
		hiveserver2_conn_id='hive_cerebro_prod',
		schema=HIVE_DB,
		)
	return task


def ssh_task(task_id, command):
	task = SSHOperator(
		task_id=task_id,
		ssh_conn_id='ssh_clv_job_submitter',
		command=command,
	)
	return task


# DAG

with DAG(dag_id='deact-model',
	schedule_interval='00 09 * * *',
	catchup=False,
	max_active_runs=2,
	default_args=default_args,
	description=__doc__,) as dag:

	################
	# TARGET TABLE #
	################

	target_data_drop_date = date_add(train_date, -TARGET_DATA_DAYS)
	verify_t365d_partition = verify_partition_task('t365d', train_date)

	target_tbl = HiveServer2HiveOperator(
		task_id='target_tbl',
		hqls='target.sql',
		hiveserver2_conn_id='hive_cerebro_prod',
		schema=HIVE_DB,
		hive_conf={
			'mapred.job.queue.name': 'clv',
			'hive.exec.copyfile.maxsize': 7516192768,
		},
		params={'feature_date': train_date, 'today': today}
		)

	target_partition_stats = partition_stats_task('target', train_date)
	drop_old_target = drop_partition_task('target', target_data_drop_date)

	####################################################
	# TRAIN DEACT MODEL, EVALUATE IT, MAKE PREDICTIONS #
	####################################################

	app_args = [
		'--train_date', train_date,
		'--train_pct', train_pct,
		]

	if calibrate_model:
		app_args.extend(['--calibrate_model', '--calibrate_pct', calibrate_pct])

	if validate_model:
		app_args.extend(['--validate_model', '--validate_pct', validate_pct])

	if score_active_users:
		app_args.extend(['--score_active_users', '--score_date', score_date])

	if mask_prob:
		app_args.extend(['--mask_prob', '--mask_days', mask_days])

	deact_model = SSHSparkSubmitOperator(
		task_id='deact_model',
		hdfs_application_file='{0}/run.py'.format(DEACT_DIR),  # Python file with Spark job
		application_args=app_args,  # Arguments for run.py file
		queue='public',
		spark_config_dict={
			'spark.app.name': 'deact-model',
			'spark.master': 'yarn',
			'spark.submit.deployMode': 'client',
			# 'spark.yarn.queue': 'public',
			'spark.driver.memory': '50g',
			'spark.executor.memory': '50g',
			'spark.executor.instances': 50,
			'spark.yarn.appMasterEnv.PYSPARK_PYTHON': PYTHON_DIR,
			'spark.yarn.dist.archives': ARCHIVES_DIR,
			'spark.sql.warehouse.dir': WAREHOUSE_DIR,
			},
		ssh_conn_id='ssh_clv_job_submitter',
		)

	if validate_model:
		papermill_command = 'papermill {0} {0} -p today {1} -p train_date {2}'\
			.format(DEACT_DIR+'/model_evaluation/Deact_Model_Eval.ipynb', today, train_date)

		refresh_jupyter = ssh_task('refresh_jupyter', papermill_command)

		git_command = '''cd {0} && git pull && git add model_evaluation/ &&
			git commit -m "refreshing evaluation results from {1}" && git push origin master'''\
			.format(DEACT_DIR, train_date)

		update_git = DummyOperator(task_id='update_git')
		# update_git = ssh_task('update_git', git_command)

	if score_active_users:
		verify_scoring_partition = verify_partition_task('scoring', score_date)
		add_predictions_partition = add_partition_task('predictions', score_date)
		add_totals_partition = add_partition_task('totals', score_date)

	#################
	# SUCCESS EMAIL #
	#################

	if calibrate_model:
		calibrate_str = '<li>Calibrated model on {0:0.0%} of the data from {1}</li>'.format(calibrate_pct, train_date)
	else:
		calibrate_str = '<li>Did not calibrate the model</li>'

	if mask_prob:
		mask_str = '<li>Applied probability mask to customers deactivating in the next {0:d} days</li>'.format(mask_days)
	else:
		mask_str = '<li>Did not apply a probability mask</li>'

	if validate_model:
		validate_str = '''<li>Validated model on {0:0.0%} of the data from {1}</li>
			<li>Saved results of model evaluation to: {2}/model_evaluation/</li>'''\
			.format(validate_pct, train_date, DEACT_DIR)  # ADD LOCATION
	else:
		validate_str = '<li>Did not validate the model</li>'

	if score_active_users:
		score_str = '''<li>Scored active users from {0}</li>
			<li>Added partition to {1}.ce_keep_deact_predictions for {0}</li>
			<li>Added partition to {1}.ce_keep_deact_totals for {0}</li>'''.format(score_date, HIVE_DB)
	else:
		score_str = '<li>Did not score active users</li>'

	success_email = EmailOperator(
		task_id='success_email',
		to=['ceasterwood@groupon.com'],
		subject='Deact Model Success {0}'.format(train_date),
		html_content='''<h3>Workflow Completed Successfully!</h3>
			<p><ol><li>Added partition to {0}.ce_keep_deact_target for {1}</li>
			<li>Removed partitions from {0}.ce_keep_deact_target for {2} and earlier</li>
			<li>Trained deact model on {3:0.0%} of the data from {1}</li>
			{4}{5}{6}{7}
			</ol></p>'''.format(HIVE_DB, train_date, target_data_drop_date, train_pct,
				calibrate_str, mask_str, validate_str, score_str),
		)

	#######################
	# ORDER OF OPERATIONS #
	#######################

	verify_t365d_partition >> target_tbl >> [target_partition_stats, drop_old_target]
	target_partition_stats >> deact_model

	if validate_model:
		deact_model >> refresh_jupyter >> update_git >> success_email
	if score_active_users:
		target_tbl >> verify_scoring_partition >> \
			[deact_model, add_predictions_partition, add_totals_partition] >> success_email
	if not validate_model and not score_active_users:
		deact_model >> success_email
