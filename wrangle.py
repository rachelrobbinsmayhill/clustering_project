from imports import *

'''
The query below is used to join 9 tables from the zillow dataset in the Codeup SQL Cloud Database.  
The tables joined are: properties_2017, predictions_2017, airconditioningtype, architecturalstyletype, 
buildingclasstype, heatingorsystemtype, propertylandusetype, storytype, typeconstructiontype. 
The data is filtered to only include the observationswith non-null latitude and longitude and with a 
transaction date occurring in 2017. 
'''

sql = """
SELECT prop.*, 
       pred.logerror, 
       pred.transactiondate, 
       air.airconditioningdesc, 
       arch.architecturalstyledesc, 
       build.buildingclassdesc, 
       heat.heatingorsystemdesc, 
       landuse.propertylandusedesc, 
       story.storydesc, 
       construct.typeconstructiondesc 
FROM   properties_2017 prop  
       INNER JOIN (SELECT parcelid,
       					  logerror,
                          Max(transactiondate) transactiondate 
                   FROM   predictions_2017 
                   GROUP  BY parcelid, logerror) pred
               USING (parcelid) 
       LEFT JOIN airconditioningtype air USING (airconditioningtypeid) 
       LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid) 
       LEFT JOIN buildingclasstype build USING (buildingclasstypeid) 
       LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid) 
       LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid) 
       LEFT JOIN storytype story USING (storytypeid) 
       LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid) 
WHERE  prop.latitude IS NOT NULL 
       AND prop.longitude IS NOT NULL AND transactiondate <= '2017-12-31' 
"""

# access the zillow data using the query above
def get_db_url(database):
    from env import host, user, password
    url = f'mysql+pymysql://{user}:{password}@{host}/{database}'
    return url


# acquire zillow data using the query above, convert it to a dataframe, and save as a .csv for faster processing.
def get_zillow():
    filename = 'zillow.csv'
    
    if os.path.exists(filename):
        print('Reading from csv file...')
        return pd.read_csv(filename)
    
    url = get_db_url('zillow')
    print('Getting a fresh copy from SQL database...')
    zillow_df = pd.read_sql(sql, url, index_col='id')
    zillow_df = zillow_df.drop_duplicates(subset = 'parcelid')
    print('Saving to csv...')
    zillow_df.to_csv(filename, index=False)
    return zillow_df



def missing_values_per_column(df):
# identifies nulls by column, creates a dataframe to display counts and percent of nulls by column
    missing_in_columns = pd.concat([
        df.isna().sum().rename('count').sort_values(ascending = False),
        df.isna().mean().rename('percent')
    ], axis=1)
    return missing_in_columns


def missing_values_per_row(df):
# identifies nulls by row, creates a dataframe to display counts and percent of nulls by row
    missing_in_rows = pd.concat([
        df.isna().sum(axis=1).rename('n_cols_missing'),
        df.isna().mean(axis=1).rename('percent_missing'),
        ], axis=1).value_counts().to_frame(name='row_counts').sort_index().reset_index()
 
    return missing_in_rows


def remove_columns(df, cols_to_remove = ['censustractandblock','finishedsquarefeet12','buildingqualitytypeid', 'heatingorsystemtypeid', 'propertyzoningdesc', 'heatingorsystemdesc', 'unitcnt']):
#removes columns that will not be used in the exploration and modeling phases of the pipeline
    df = df.drop(columns=cols_to_remove)
    return df


def handle_missing_values(df, prop_required_column = .5, prop_required_row = .5):
# Drops missing values based upon a set threshold. It filters columns first and then rows, dropping columns and rows with more than 50% missing data. 
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df



#Adjusted prep function, taking out the remove function. The original data_prep is below. 
def data_prep(df, prop_required_column=.5, prop_required_row=.5):
    
    df = handle_missing_values(df, prop_required_column, prop_required_row)
   
    # Make categorical column for location based upon the name of the county that belongs to the cooresponding state_county_code (fips code)
    df['county_code_bin'] = pd.cut(df.fips, bins=[0, 6037.0, 6059.0, 6111.0], 
                             labels = ['Los Angeles County', 'Orange County',
                             'Ventura County'])
   
    # Make dummy columns for state_county_code using the binned column for processin gin modeling later. 
    dummy_df = pd.get_dummies(df[['county_code_bin']], dummy_na=False, drop_first=[True])
    
    # Add dummy columns to dataframe
    df = pd.concat([df, dummy_df], axis=1)

    # Make categorical column for square_feet.
    df['home_sizes'] = pd.cut(df.calculatedfinishedsquarefeet, bins=[0, 1800, 4000, 6000, 25000], 
                             labels = ['Small: 0 - 1799sqft',
                             'Medium: 1800 - 3999sqft', 'Large: 4000 - 5999sqft', 'Extra-Large: 6000 - 25000sqft'])
    
    # Make categorical column for total_rooms, combining number of bedrooms and bathrooms.
    df['total_rooms'] = df['bedroomcnt'] + df['bathroomcnt']
    
    # Make categorical column for bedrooms.
    df['bedroom_bins'] = pd.cut(df.bedroomcnt, bins=[0, 2, 4, 6, 15], 
                             labels = ['Small: 0-2 bedrooms',
                             'Medium: 3-4 bedrooms', 'Large: 5-6 bedrooms', 'Extra-Large: 7-15 bedrooms'])
    
    # Make categorical column for square_feet.
    df['bathroom_bins'] = pd.cut(df.bathroomcnt, bins=[0, 2, 4, 6, 15], 
                             labels = ['Small: 0-2 bathrooms','Medium: 3-4 bathrooms', 'Large: 5-6 bathrooms', 
                                       'Extra-Large: 8-15 bathrooms'])
    df = df.dropna()
    print(df.shape)
    return df



def summary_info(df): 
    # Summarize data (shape, info, summary stats, dtypes, shape)
    print('--- Shape: {}'.format(df.shape))
    print('--- Descriptions')
    print(df.describe(include='all'))
    print('--- Info')
    return df.info()








'''
def data_prep(df, cols_to_remove=['censustractandblock','finishedsquarefeet12','buildingqualitytypeid', 'heatingorsystemtypeid', 'propertyzoningdesc', 'heatingorsystemdesc', 'unitcnt'], prop_required_column=.5, prop_required_row=.5):
    df = remove_columns(df, cols_to_remove)
    df = handle_missing_values(df, prop_required_column, prop_required_row)
   
    # Make categorical column for location based upon the name of the county that belongs to the cooresponding state_county_code (fips code)
    df['county_code_bin'] = pd.cut(df.fips, bins=[0, 6037.0, 6059.0, 6111.0], 
                             labels = ['Los Angeles County', 'Orange County',
                             'Ventura County'])
   
    # Make dummy columns for state_county_code using the binned column for processin gin modeling later. 
    dummy_df = pd.get_dummies(df[['county_code_bin']], dummy_na=False, drop_first=[True])
    
    # Add dummy columns to dataframe
    df = pd.concat([df, dummy_df], axis=1)

    # Make categorical column for square_feet.
    df['home_sizes'] = pd.cut(df.calculatedfinishedsquarefeet, bins=[0, 1800, 4000, 6000, 25000], 
                             labels = ['Small: 0 - 1799sqft',
                             'Medium: 1800 - 3999sqft', 'Large: 4000 - 5999sqft', 'Extra-Large: 6000 - 25000sqft'])
    
    # Make categorical column for total_rooms, combining number of bedrooms and bathrooms.
    df['total_rooms'] = df['bedroomcnt'] + df['bathroomcnt']
    
    # Make categorical column for bedrooms.
    df['bedroom_bins'] = pd.cut(df.bedroomcnt, bins=[0, 2, 4, 6, 15], 
                             labels = ['Small: 0-2 bedrooms',
                             'Medium: 3-4 bedrooms', 'Large: 5-6 bedrooms', 'Extra-Large: 7-15 bedrooms'])
    
    # Make categorical column for square_feet.
    df['bathroom_bins'] = pd.cut(df.bathroomcnt, bins=[0, 2, 4, 6, 15], 
                             labels = ['Small: 0-2 bathrooms','Medium: 3-4 bathrooms', 'Large: 5-6 bathrooms', 
                                       'Extra-Large: 8-15 bathrooms'])
    df = df.dropna()
    print(df.shape)
    return df.head()

'''
def single_family_homes(df):
    # Restrict df to only properties that meet single unit criteria
    #261: Single Family Residential, #262: Rural Residence, #263: Mobile Homes, 
    #264: Townhomes, #265 Cluster Homes, #266: Condominium, #268: Row House, 
    #273 Bungalow, #275 Manufactured, #276 Patio Home, #279 Inferred Single Family Residence

    single_use = [261, 262, 263, 264, 265, 266, 268, 273, 275, 276, 279]
    df = df[df.propertylandusetypeid.isin(single_use)]

    # Restrict df to only those properties with at least 1 bath & bed and > 400 sqft area (to not include tiny homes)
    
    df = df[(df.bedroomcnt > 0) & (df.bathroomcnt > 0) & ((df.unitcnt<=1)|df.unitcnt.isnull()) & (df.calculatedfinishedsquarefeet>400)]

    return df







def split(df):
    train_and_validate, test = train_test_split(df, random_state=123, test_size=.15)
    train, validate = train_test_split(train_and_validate, random_state=123, test_size=.2)

    print('Train: %d rows, %d cols' % train.shape)
    print('Validate: %d rows, %d cols' % validate.shape)
    print('Test: %d rows, %d cols' % test.shape)

    return train, validate, test









