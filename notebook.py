#!/usr/bin/env python
# coding: utf-8

# # Ingest Tabular Data
# 
# 

# ## Download data from online resources and write data to S3

# In[36]:


import pandas as pd
import boto3
import csv
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure

get_ipython().run_line_magic('matplotlib', 'inline')

client = boto3.client('iotanalytics')


# In[37]:


dataset_url = client.get_dataset_content(datasetName = 'vehicleemissions_dataset')['entries'][0]['dataURI']
data = pd.read_csv(dataset_url)

# Data Formatted 
#
# message     Process Emission! , 2416.04 : 0
# vehicle0                            9280.18
# vehicle1                            16132.9
# vehicle2                            11307.7
# vehicle3                            7074.08
# vehicle4                            13855.7
# count                               1
# __dt                2022-04-22 00:00:00.000
# Name: 0, dtype: object


# In[38]:


vehicle0 = []
vehicle1 = []
vehicle2 = []
vehicle3 = []
vehicle4 = []
count = []
        
# Match length of dataset
for i in range(50):
    count.append(data.iloc[i][6])
    vehicle0.append(data.iloc[i][1])
    vehicle1.append(data.iloc[i][2])
    vehicle2.append(data.iloc[i][3])
    vehicle3.append(data.iloc[i][4])
    vehicle4.append(data.iloc[i][5])

print(vehicle0,end="\n\n")
print(vehicle1,end="\n\n")
print(vehicle2,end="\n\n")
print(vehicle3,end="\n\n")
print(vehicle4,end="\n\n")


# In[39]:


plt.rcParams["figure.figsize"] = (20,6)
plt.title("Co2 Emissions over Time for 5 Vehicles")
plt.plot(vehicle0, 'b', label="vehicle0")
plt.plot(vehicle1, 'r', label="vehicle1")
plt.plot(vehicle2, 'k', label="vehicle2")
plt.plot(vehicle3, 'y', label="vehicle3")
plt.plot(vehicle4, 'g', label="vehicle4")
plt.ylabel('Co2 Emissions')
plt.xlabel('Time')
plt.legend()
plt.show()


# In[40]:


# Pie chart, where the slices will be ordered and plotted counter-clockwise:
plt.rcParams["figure.figsize"] = (20,10)
labels = ['Vehicle0', 'Vehicle1', 'Vehicle2', 'Vehicle3', 'Vehicle4']
total = vehicle0[-1] + vehicle1[-1] + vehicle2[-1] + vehicle3[-1] + vehicle4[-1]
sizes = [(vehicle0[-1]/total*100), (vehicle1[-1]/total*100), (vehicle2[-1]/total*100), (vehicle3[-1]/total*100), (vehicle4[-1]/total*100)]
explode = (0, 0, 0.1, 0, 0)  # only "explode" the 2nd slice (i.e. 'Hogs')

fig1, ax1 = plt.subplots()
ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=90)
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.title("Percentage per Vehicle of Total Vehicle Emissions")

plt.show()


# In[ ]:




