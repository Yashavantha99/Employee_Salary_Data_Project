# %% [markdown]
# # Install Libraries 

# %%
pip install pandas sqlalchemy oracledb datetime


# %% [markdown]
# # Extract Data from Kaggle CSV (dataset) -- EXTRACTION

# %%
import kaggle
kaggle.api.authenticate()
kaggle.api.dataset_download_files('youvolvedata/employee-salary-data', path = 'C:/Users/yashv/Desktop/Employee_salary_data_project', unzip = True)

# %% [markdown]
# ### Import dataset to dataframe

# %%
import pandas as pd

#Loading the dataset
df = pd.read_csv("C:/Users/yashv/Desktop/Employee_salary_data_project/emp_salary_data.csv")
df.head()

# %%
df['department'].unique()        #finding unique departments

# %%
df['first_name'] = df['first name'].str.isalpha()       #finding errors in first name 
df['last_name'] = df['last name'].str.isalpha()         #finding errors in last name
df['monthly_salary'] = df['monthly salary'].str.isnumeric()  #finding errors in monthly salary

# %% [markdown]
# # Transformation on data begins

# %% [markdown]
# ### Rows with alphanumeric data issues

# %%
df2 = df.query('first_name == False or last_name == False or monthly_salary ==False')
df2

# %% [markdown]
# ### Modifying data issues

# %%
df['first name'] = df['first name'].str.replace('#','')
df['first name'] = df['first name'].str.replace('$','')
df['last name'] = df['last name'].str.replace('+','')
df['last name'] = df['last name'].str.replace('$','')
df['monthly salary'] = df['monthly salary'].str.replace('&','')
df['monthly salary'] = df['monthly salary'].str.replace(' ','')

# %%
df.drop(['first_name', 'last_name', 'monthly_salary'], axis =1, inplace=True)

# %% [markdown]
# ### Rows with alphanumeric data issue -- fixed

# %%
df

# %% [markdown]
# ### Checing join date dataype

# %%
df['join date2'] = df['join date']
df['join date'] = pd.to_datetime(df['join date'], format = "%d-%b-%Y", errors = 'coerce')

# %% [markdown]
# ### data with invalid date datatype in join date

# %%
df3 = df[df['join date'].isna()]
df3

# %%
df.drop('join date2', axis = 1, inplace=True)    

# %%
df

# %%
df = df.dropna()

# %%
df    #dropped null values

# %% [markdown]
# ### Fix the column header space

# %%
df.columns = df.columns.str.replace(' ','_')

# %%
df = df.copy()
df['birth_date'] = pd.to_datetime(df['birth_date'], format='%d-%b-%Y')

# %% [markdown]
# # Loading the cleaned dataset to the Oracle database using Oralcedb and SQLAlchemy

# %% [markdown]
# ###  Connect to Oracle Database using oracledb + sqlalchemy

# %%
import sqlalchemy as odb

# %%
username = 'your_database_username'
password = 'your_datasbase_password'
dsn = 'host_name:port_number/?service_name=service_name'
conn_str = 'oracle+oracledb://'+username+':'+password+'@'+dsn
print(conn_str)
eng = odb.create_engine(conn_str)
df.to_sql('employee_salary_data', con= eng, if_exists='append', index = False)

# %%
# Save query result to CSV file
df.to_csv('C:/Users/yashv/Desktop/Employee_salary_data_project/src/datasets/cleaned_dataset.csv', index=False)

print("Exported cleaned data successfully!")


# %%



