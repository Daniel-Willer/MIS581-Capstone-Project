#!/usr/bin/env python
# coding: utf-8

# In[22]:


import pandas as pd
import datetime
import os
import subprocess
import sys
from sklearn.model_selection import train_test_split
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.tree import DecisionTreeClassifier
from sklearn import metrics
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
get_ipython().run_line_magic('matplotlib', 'inline')


# In[ ]:





# In[23]:


df = pd.read_csv('/Users/danielwiller/Documents/CSU/MIS581/MIS581 Capstone Project/MIS581_Capstone_Project_ad_unit_data.csv')


# In[24]:


df


# In[25]:


today_str = datetime.datetime.today().strftime('%Y-%m-%d')


# In[26]:


list(df.columns)


# In[27]:


# Create features and target variables
feature_cols = ['grss_bkd_amt', 'network_code', 'dy_prt_id', 'inv_lnth_in_sec'
                , 'inv_typ_cd','avails','units_cleared','mean_aired_rate'
                ,'median_aired_rate']
X = df[feature_cols] # Features
y = df.aird_ind # Target variable


# In[28]:


# Partition Data into traing and validation sets
X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.25,random_state=0)


# In[29]:


# Use Logistic Regression to predict aired status 
# instantiate the model (using the default parameters)
logreg = LogisticRegression(max_iter=1000)

# fit model to ad unit data 
logreg.fit(X_train,y_train)
y_pred=logreg.predict(X_test)

cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
print("Confusion Matrix:")
print(cnf_matrix)

accuracy = metrics.accuracy_score(y_test, y_pred)
precision = metrics.precision_score(y_test, y_pred)
recall = metrics.recall_score(y_test, y_pred)
y_pred_proba = logreg.predict_proba(X_test)[::,1]
fpr, tpr, _ = metrics.roc_curve(y_test,  y_pred_proba)
auc = metrics.roc_auc_score(y_test, y_pred_proba)


print("\nModel Performance:")
print("Accuracy:",accuracy)
print("Precision:",precision)
print("Recall:",recall)
print("AUC:",auc)


# In[30]:


#Heat Map Confusion Matrix
class_names=[0,1] # name  of classes
fig, ax = plt.subplots()
tick_marks = np.arange(len(class_names))
plt.xticks(tick_marks, class_names)
plt.yticks(tick_marks, class_names)
# create heatmap
sns.heatmap(pd.DataFrame(cnf_matrix), annot=True, cmap="YlGnBu" ,fmt='g')
ax.xaxis.set_label_position("top")
plt.tight_layout()
plt.title('Confusion matrix', y=1.1)
plt.ylabel('Actual label')
plt.xlabel('Predicted label')


# In[31]:


# Write data to .csv for use in Tableau
tn = cnf_matrix[0, 0]
fn = cnf_matrix[0, 1]
fp = cnf_matrix[1, 0]
tp = cnf_matrix[1, 1]
roc_curve_df = pd.DataFrame({'fpr':fpr, 'tpr':tpr})
roc_curve_df['updated'] = today_str
measure_vals = {"accuracy": [accuracy], "precision": [precision], "recall": [recall], "auc": [auc], "tn": [tn], "fn": [fn], "fp": [fp], "tp": [tp], "update": [today_str] }
measure_vals_df = pd.DataFrame.from_dict(measure_vals, orient='columns')

roc_curve_df.to_excel('/Users/danielwiller/Documents/CSU/MIS581/MIS581 Capstone Project/lr_roc_curve.xlsx')
measure_vals_df.to_excel('/Users/danielwiller/Documents/CSU/MIS581/MIS581 Capstone Project/lr_metics.xlsx')


# In[32]:



# Create Decision Tree classifer object
clf = DecisionTreeClassifier()

# Train Decision Tree Classifer
clf = clf.fit(X_train,y_train)

# Predict the response for test dataset
y_pred = clf.predict(X_test)

# Measure Model Accuarcy
# Need to add logic to write the accuracy dataset 
accuracy = metrics.accuracy_score(y_test, y_pred)
precision = metrics.precision_score(y_test, y_pred)
recall = metrics.recall_score(y_test, y_pred)
y_pred_proba = clf.predict_proba(X_test)[::,1]
fpr, tpr, _ = metrics.roc_curve(y_test,  y_pred_proba)
auc = metrics.roc_auc_score(y_test, y_pred_proba)
cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
print(cnf_matrix)


print("Model Performance:")
print("Accuracy:",accuracy)
print("Precision:",precision)
print("Recall:",recall)
print("AUC:",auc)


# In[33]:


#Heat Map Confusion Matrix
class_names=[0,1] # name  of classes
fig, ax = plt.subplots()
tick_marks = np.arange(len(class_names))
plt.xticks(tick_marks, class_names)
plt.yticks(tick_marks, class_names)
# create heatmap
sns.heatmap(pd.DataFrame(cnf_matrix), annot=True, cmap="YlGnBu" ,fmt='g')
ax.xaxis.set_label_position("top")
plt.tight_layout()
plt.title('Confusion matrix', y=1.1)
plt.ylabel('Actual label')
plt.xlabel('Predicted label')


# In[34]:


# Write data to .csv for use in Tableau
tn = cnf_matrix[0, 0]
fn = cnf_matrix[0, 1]
fp = cnf_matrix[1, 0]
tp = cnf_matrix[1, 1]
roc_curve_df = pd.DataFrame({'fpr':fpr, 'tpr':tpr})
roc_curve_df['updated'] = today_str
measure_vals = {"accuracy": [accuracy], "precision": [precision], "recall": [recall], "auc": [auc], "tn": [tn], "fn": [fn], "fp": [fp], "tp": [tp], "update": [today_str] }
measure_vals_df = pd.DataFrame.from_dict(measure_vals, orient='columns')

roc_curve_df.to_excel('/Users/danielwiller/Documents/CSU/MIS581/MIS581 Capstone Project/dt_roc_curve.xlsx')
measure_vals_df.to_excel('/Users/danielwiller/Documents/CSU/MIS581/MIS581 Capstone Project/dt_metics.xlsx')


# In[ ]:





# In[ ]:




