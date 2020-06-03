
# In[1]:


from datetime import datetime
print("[",str(datetime.now()),"]",": Deactivation model 2.0 triggered")
a = datetime.now()
import pandas as pd
import numpy as np
from scipy.stats import mode
from sklearn.model_selection import train_test_split
from pyhive import hive

import xgboost as xgb
import itertools
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import PolynomialFeatures
from xgboost.sklearn import XGBClassifier
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from scipy.stats import randint as sp_randint
#import pickle as pkl

import matplotlib.pylab as plt
# %matplotlib inline
from matplotlib.pylab import rcParams
# rcParams['figure.figsize'] = 14, 7

# pd.set_option('display.width', 5000) 
# pd.set_option('display.max_columns', 60)
# pd.set_option('display.max_rows', 100)

# %matplotlib inline

# pd.set_option('display.width', 5000) 
# pd.set_option('display.max_columns', 60)
# pd.set_option('display.max_rows', 100)


# In[2]:


print("[",str(datetime.now()),"]",": Reading training data from Cerebro...")

conn = hive.Connection(host="cerebro-hive-server1.snc1", port=10000, username="ebates")
query ='''
select *
from grp_gdoop_clv_db.eb_pip_deact_all_features
where record_date = date_sub(current_date, 2)
and rand() <= 0.5
distribute by rand() --distributes randomly across amps
sort by rand()
limit 10000000
'''

df_raw = pd.read_sql(query,conn)
print("[",str(datetime.now()),"]",": Training data read complete. Feature engineering started...")


# In[3]:


df_raw = df_raw.rename(columns={col: col.split('.')[1] for col in df_raw.columns})
# df_raw


# In[4]:


training_date = pd.Timestamp(df_raw.record_date.min())
print ("Training date:",training_date.date())


# In[5]:


# df_raw.describe()


# In[6]:


# df_raw.deact_flag.value_counts()


# In[7]:


# col_list = list(df_raw.columns)
# col_list


# In[8]:


df_raw = df_raw.drop(['record_date', 'brand'], axis = 1)


# In[9]:


len(df_raw.columns)


# In[10]:


#last visit date and last_purchase_date (deact_date - 365) working
df_raw.deact_date  = pd.to_datetime(df_raw.deact_date)
df_raw.last_visit_date  = pd.to_datetime(df_raw.last_visit_date)
# df_2.next_order_date = pd.to_datetime(df_2.next_order_date)


# In[11]:


df_raw['last_purchase_date'] = df_raw['deact_date'].apply(lambda x: x - pd.DateOffset(years=1))


# In[12]:


df_raw['gap_purchase_last_visit'] = (df_raw['last_visit_date'] - df_raw['last_purchase_date']).dt.days


# In[13]:


df_raw['days_till_deactivation'] = (df_raw['deact_date'] - training_date).dt.days


# In[14]:


df_raw = df_raw.dropna(subset = ['deact_date', 'tenure_days'])


# In[15]:


df_raw.describe()


# In[16]:


# col_list_3 = df_raw.columns


# In[17]:


#solving NaN
df_raw['last_visit_date'] = df_raw['last_visit_date'].fillna(df_raw['last_purchase_date'] -  pd.to_timedelta(90, unit='d'))
df_raw = df_raw.fillna({'visit_recency':90, 'bcookies': 1, 'send_recency':30, 'open_recency':30, 'click_recency':30, 'gap_purchase_last_visit': 0})
df_raw['gap_purchase_last_visit'] = df_raw.gap_purchase_last_visit.clip_lower(0)


# In[18]:


df_raw = pd.get_dummies(df_raw, columns = ['recency_segment', 'frequency_segment', 'recency_9block', 'frequency_9block','most_recent_l1','most_recent_l2', 'most_recent_platform', 'most_recent_promo_type'])


# In[19]:


len(list(df_raw.columns))


# In[20]:


df_raw['deact_dayofmonth'] = df_raw.deact_date.dt.day
df_raw['deact_dayofyear'] = df_raw.deact_date.dt.dayofyear
df_raw['deact_dayofweek'] = df_raw.deact_date.dt.dayofweek
df_raw['deact_weekofyear'] = df_raw.deact_date.dt.weekofyear
df_raw['deact_month'] = df_raw.deact_date.dt.month
df_raw['deact_weekday'] = ((df_raw.deact_date.dt.dayofweek) // 5 == 1).astype(float)


# In[21]:


df_raw['last_purchase_dayofmonth'] = df_raw.last_purchase_date.dt.day
df_raw['last_purchase_dayofyear'] = df_raw.last_purchase_date.dt.dayofyear
df_raw['last_purchase_dayofweek'] = df_raw.last_purchase_date.dt.dayofweek
df_raw['last_purchase_weekofyear'] = df_raw.last_purchase_date.dt.weekofyear
df_raw['last_purchase_month'] = df_raw.last_purchase_date.dt.month
df_raw['last_purchase_weekday'] = ((df_raw.last_purchase_date.dt.dayofweek) // 5 == 1).astype(float)


# In[22]:


str(training_date.date())


# In[23]:


print("[",str(datetime.now()),"]",": Feature engineering completed. Model training started...")


# In[24]:


df_raw


# In[25]:


y = range(df_raw.shape[0])


# In[26]:


len(y)


# In[27]:


factor = 0.90
print ("Training records: ", int(len(y)*(1-factor)))


# In[28]:


df_3, df_raw, y_train, y = train_test_split(df_raw, y, test_size=factor, random_state=42)


# In[29]:


training_date = df_3.deact_date.max()


# In[30]:


# df_training_sample.to_csv('Model_evaluation_data_'+str(training_date) + '.csv', index = False)


# In[31]:


def acc_calc(X, y, model_trained):
    print ("\nModel Report")
    y_pred = model_trained.predict(X)
    fpr, tpr, _ = metrics.roc_curve(y.values, y_pred)
    print ("Accuracy : %.4g" % metrics.accuracy_score(y.values, y_pred))
    print ("AUC Score (Train): %f" % metrics.auc(fpr, tpr))

    score = metrics.average_precision_score(y.values, y_pred)
    print('Area under the precision-recall curve: {:.6f}'.format(score))
    #ROC Curve
    #xgb.plot_importance(gbm)
    #plt.show()
#     plt.figure()
#     lw = 3
#     plt.plot(fpr, tpr, color='red',
#              lw=lw, label='ROC curve (area = %0.2f)' % metrics.auc(fpr, tpr))
#     plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
#     plt.xlim([-0.02, 1.0])
#     plt.ylim([0.0, 1.05])
#     plt.xlabel('False Positive Rate')
#     plt.ylabel('True Positive Rate')
#     plt.title('ROC curve')
#     plt.legend(loc="lower right")
#     plt.show()

    feat_imp = pd.Series(model_trained.get_booster().get_fscore()).sort_values(ascending=False)
#     feat_imp.plot(kind='bar', title='Feature Importances')
#     plt.ylabel('Feature Importance Score')
    return feat_imp


# In[32]:


df_3 = df_3.dropna(thresh=50)


# In[33]:


# df_3.isna().sum()


# In[34]:


# x_train, x_test, y_train, y_test = train_test_split(X, y, test_size =0.15, shuffle = True, random_state = 42)


# Iteration 1

# In[35]:


target = 'deact_flag'
IDcol = 'consumer_id'
not_predictors = [target
                 , IDcol
                 , 'deact_date'
                 , 'next_order_date'
                 , 'last_visit_date'
                 , 'last_purchase_date'
                 , 'data_set'
                 ]
predictors = [x for x in df_3.columns if x not in not_predictors]


# In[37]:


xgb1 = XGBClassifier(
learning_rate =0.1,
n_estimators=1000,
max_depth=6,
min_child_weight=5,
gamma=0.05,
subsample=0.8,
colsample_bytree=0.8,
objective= 'binary:logistic',
n_jobs=20,
reg_lambda = 0.9,
scale_pos_weight=1,
silent = 1,
missing= np.nan,
seed=5)
xgb1.fit(df_3[predictors], df_3[target], eval_metric='auc', verbose = False)


# In[ ]:


feat_imp = acc_calc(df_3[predictors], df_3[target], xgb1)


# In[ ]:


# Dump the model
# pkl.dump(xgb1, open("Deact_20180701_dt_1.pickle.dat", "wb"))
# pkl.dump(xgb_final, open("Deact.pickle.dat", "wb"))


# NEXT ITERATION 2

# In[ ]:


least_favourable = list(pd.Series(feat_imp[-20:]).index)
print ("20 least important features:\n")
print (least_favourable)
not_predictors.extend(least_favourable)
predictors = [x for x in df_3.columns if x not in not_predictors]
print ("\nPredictors left: %i " %len(predictors))


# In[ ]:


print("[",str(datetime.now()),"]",": Iteration 1 completed. Iteration 2 started...")


# In[ ]:


# xgb2 = XGBClassifier(
# learning_rate =0.1,
# n_estimators=1000,
# max_depth=7,
# min_child_weight=1,
# gamma=0,
# subsample=0.8,
# colsample_bytree=0.8,
# objective= 'binary:logistic',
# n_jobs=10,
# scale_pos_weight=1,
# silent = 0,
# seed=5,
# reg_alpha=0.4)
xgb1.fit(df_3[predictors], df_3[target], eval_metric='auc', verbose = False)


# In[ ]:


feat_imp = acc_calc(df_3[predictors], df_3[target], xgb1)


# In[ ]:


# Dump the model
# pkl.dump(xgb2, open("Deact_20180624_dt_2.pickle.dat", "wb"))
# pkl.dump(xgb_final, open("Deact.pickle.dat", "wb"))


# FINAL TRAINING

# In[ ]:


least_favourable = list(pd.Series(feat_imp[-10:]).index)
print ("10 least important features:\n")
print (least_favourable)
not_predictors.extend(least_favourable)
predictors = [x for x in df_3.columns if x not in not_predictors]
print ("\nPredictors left: %i " %len(predictors))


# In[ ]:


print("[",str(datetime.now()),"]",": Iteration 2 completed. Iteration 3 started...")


# In[ ]:


predictors = [x for x in df_3.columns if x not in not_predictors]
not_pred = pd.DataFrame(not_predictors, columns = ["not_predictor"])
# not_pred.to_csv('not_pred.csv', index = False)
pred = pd.DataFrame(predictors, columns = ["predictor"])
# pred.to_csv('pred.csv', index = False)


# In[ ]:


print ("Predictors included in model: ", list(predictors))
print ("Predictors not included in model: ", list(not_predictors))


# In[ ]:


xgb1 = XGBClassifier(
learning_rate =0.01,
n_estimators=5000,
max_depth=10,
min_child_weight=4,
gamma=0.05,
subsample=1,
colsample_bytree=1,
objective= 'binary:logistic',
n_jobs=20,
scale_pos_weight=1,
silent = 0,
seed=5)
xgb1.fit(df_3[predictors], df_3[target], eval_metric='auc', verbose = False)


# In[ ]:


feat_imp = acc_calc(df_3[predictors], df_3[target], xgb1)


# In[ ]:


print("[",str(datetime.now()),"]",": Training completed. Saving the trained model...")


# In[ ]:


# Dump the model
#pkl.dump(xgb1, open("Deact_model_2.dat", "wb"))


# In[ ]:


print("[",str(datetime.now()),"]",": Model saved. Reading prediction (scoring) data from cerebro...")

conn = hive.Connection(host="cerebro-hive-server1.snc1", port=10000, username="sth")
query ='''
select *
from grp_gdoop_clv_db.eb_pip_deact_all_features_scoring
where record_date = date_sub(current_date, -363)
'''

df_raw = pd.read_sql(query,conn)

print("[",str(datetime.now()),"]",": Prediction (scoring) data loaded. Feature engineering started...")


# In[ ]:


df_raw


# In[ ]:


df_raw = df_raw.rename(columns={col: col.split('.')[1] for col in df_raw.columns})


# In[ ]:


training_date = pd.Timestamp(df_raw.record_date.min())
print ("Prediction end date: ",training_date)
print (len(df_raw), " records loaded.")


# In[ ]:


col_list = df_raw.columns


# In[ ]:


df_raw = df_raw.drop(['record_date', 'brand'], axis = 1)


# In[ ]:


len(df_raw.columns)


# In[ ]:


#last visit date and last_purchase_date (deact_date - 365) working
df_raw.deact_date  = pd.to_datetime(df_raw.deact_date)
df_raw.last_visit_date  = pd.to_datetime(df_raw.last_visit_date)
# df_2.next_order_date = pd.to_datetime(df_2.next_order_date)


# In[ ]:


df_raw['last_purchase_date'] = df_raw['deact_date'].apply(lambda x: x - pd.DateOffset(years=1))


# In[ ]:


df_raw['gap_purchase_last_visit'] = (df_raw['last_visit_date'] - df_raw['last_purchase_date']).dt.days


# In[ ]:


df_raw['days_till_deactivation'] = (df_raw['deact_date'] - training_date).dt.days


# In[ ]:


#df_raw = df_raw.dropna(subset = ['deact_date', 'tenure_days'])
df_raw = df_raw.dropna(subset = ['deact_date'])

# In[ ]:


df_raw.describe()


# In[ ]:


col_list_3 = df_raw.columns


# In[ ]:


#solving NaN
df_raw['last_visit_date'] = df_raw['last_visit_date'].fillna(df_raw['last_purchase_date'] -  pd.to_timedelta(90, unit='d'))
df_raw = df_raw.fillna({'visit_recency':90, 'bcookies': 1, 'send_recency':30, 'open_recency':30, 'click_recency':30, 'gap_purchase_last_visit': 0})
df_raw['gap_purchase_last_visit'] = df_raw.gap_purchase_last_visit.clip_lower(0)


# In[ ]:


len(list(df_raw.columns))


# In[ ]:


df_raw['deact_dayofmonth'] = df_raw.deact_date.dt.day
df_raw['deact_dayofyear'] = df_raw.deact_date.dt.dayofyear
df_raw['deact_dayofweek'] = df_raw.deact_date.dt.dayofweek
df_raw['deact_weekofyear'] = df_raw.deact_date.dt.weekofyear
df_raw['deact_month'] = df_raw.deact_date.dt.month
df_raw['deact_weekday'] = ((df_raw.deact_date.dt.dayofweek) // 5 == 1).astype(float)


# In[ ]:


df_raw['last_purchase_dayofmonth'] = df_raw.last_purchase_date.dt.day
df_raw['last_purchase_dayofyear'] = df_raw.last_purchase_date.dt.dayofyear
df_raw['last_purchase_dayofweek'] = df_raw.last_purchase_date.dt.dayofweek
df_raw['last_purchase_weekofyear'] = df_raw.last_purchase_date.dt.weekofyear
df_raw['last_purchase_month'] = df_raw.last_purchase_date.dt.month
df_raw['last_purchase_weekday'] = ((df_raw.last_purchase_date.dt.dayofweek) // 5 == 1).astype(float)


# In[ ]:


str(training_date.date())


# In[ ]:


print("[",str(datetime.now()),"]",": Feature engineering completed. Predicting the deactivation probability...")


# In[ ]:


#loaded_model = pkl.load(open("Deact_model_2.dat", "rb"))
loaded_model = xgb1

# In[ ]:


loaded_model


# In[ ]:


# df_val = df_raw.copy()
# df_val = pd.read_csv('cleaned_data_training_20180803.csv')
# df_2 = pd.read_csv('cleaned_data_validation_20180803.csv')


# In[ ]:


df_raw_cols = list(df_raw.columns)


# In[ ]:


#pred = pd.read_csv('pred.csv')
#predictors = list(pred.predictor)


# In[ ]:


# df_val = pd.concat([df_val, df_2])


# In[ ]:


df_raw.shape


# In[ ]:


df_raw.deact_date.max()


# In[ ]:


df_raw.days_till_deactivation.max()


# In[ ]:


missing_cols = np.setdiff1d(predictors, df_raw_cols)


# In[ ]:


for col in missing_cols:
    df_raw[col] = np.nan


# In[ ]:


df_raw


# In[ ]:


df_2 = loaded_model.predict_proba(df_raw[predictors])


# In[ ]:


y_prob_deact=np.array(df_2)[:,1]


# In[ ]:


df_out_validation = pd.DataFrame(list(zip(df_raw['consumer_id'], df_raw['deact_date'], y_prob_deact)), columns=['consumer_id', 'Deact_date', 'Prob_deact'])


# In[ ]:


df_deact_sum = df_out_validation[['Deact_date','Prob_deact']]
df_deact_sum = df_deact_sum.groupby(df_deact_sum['Deact_date']).sum()
df_deact_sum.reset_index(inplace=True)


# In[ ]:


# df_deact_sum


# In[ ]:


# df_out_validation


# In[ ]:


print("[",str(datetime.now()),"]",": Prediction completed. Saving the output...")


# In[ ]:


# df_deact_sum.to_csv('xgb_prediction_for_20180723.csv', index = False)
df_deact_sum.to_csv('/home/ebates/Forecast_deacts_agg_by_day.csv', index = False)


# In[ ]:


# df_out_validation.to_csv('deact_model_output_test_20180723.csv', index = False)
df_out_validation.to_csv('/home/ebates/Deact_predicted_probability_by_customer.csv', index = False)


# In[ ]:


print("[",str(datetime.now()),"]",": Output saved. Pushing output to cerebro...")


# In[ ]:



import subprocess

process = subprocess.Popen(['hive','-e','"LOAD DATA LOCAL INPATH \'/home/ebates/Deact_predicted_probability_by_customer.csv\' OVERWRITE INTO TABLE grp_gdoop_clv_db.eb_deact_predictions;"','>','/home/ebates/hive_output'], stdout=subprocess.PIPE)
output, error = process.communicate()

output
error


# In[ ]:


process = subprocess.Popen(['hive','-e','"LOAD DATA LOCAL INPATH \'/home/ebates/Forecast_deacts_agg_by_day.csv\' OVERWRITE INTO TABLE grp_gdoop_clv_db.eb_deact_forecast;"','>','/home/ebates/hive_output'], stdout=subprocess.PIPE)
output, error = process.communicate()

output
error


# In[ ]:

import subprocess

process = subprocess.Popen(['hive','-e','"set hive.exec.dynamic.partition.mode=nonstrict; insert overwrite table grp_gdoop_clv_db.eb_deact_predictions1 partition(ds) select a.*, current_date as ds from grp_gdoop_clv_db.eb_deact_predictions a where date_sub(current_date,1) <= deact_date sort by deact_date"'], stdout=subprocess.PIPE)
output, error = process.communicate()

output
error

# In[ ]:

import subprocess

process = subprocess.Popen(['hive','-e','"set hive.exec.dynamic.partition.mode=nonstrict; insert overwrite table grp_gdoop_clv_db.eb_deact_forecast1 partition(ds) select a.*, current_date as ds from grp_gdoop_clv_db.eb_deact_forecast A where date_sub(current_date,1) <= deact_date sort by deact_date"'], stdout=subprocess.PIPE)
output, error = process.communicate()

output
error



b = datetime.now()
print("[",str(datetime.now()),"]",": Output loaded to Hive table succesfully. Deactivation model prediction completed.")


# In[ ]:


print ("Total time taken:", (b-a))
