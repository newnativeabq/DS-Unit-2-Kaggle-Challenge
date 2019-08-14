'''
Wrangle Pipeline
A customizeable feature transformation and engineering framework in Pandas

WranglePipe takes in full dataframe.  Individual transformations may be specified with views
    (e.g. list of features to transform/calculate with) to improve performance.

__author__ = 'Vincent Brandon'
__created_date = '2019-08-13'

'''


from collections import OrderedDict 
import pandas as pd

class WranglePipe():
    '''
    Transform Handler class mimicks sklearn make_pipeline to allow custom functions of type:
    
    input(X) -> fit(x) -> transform(x)
    '''
    def __init__(self,  **kwargs):
        self.targets = None
        self.features = None
        self.cache = None
        self.steps = OrderedDict()
        if kwargs:
            for item in kwargs.items():
                self.add(item)
    
    def add(self, step_to_add):
        if type(step_to_add) == tuple:
            (key, val) = step_to_add
            self.steps[key] = val
        elif type(step_to_add) == dict:
            for key, val in step_to_add.items():
                self.steps[key] = val
    
    def remove(self, *args):
        for key in args:
            del self.steps[key]
    
    def fit(self, X=None, y=None):     
        self.initialize_data(X, y)
        for step_name, step_function in self.steps.items():
            try:
                step_function.fit(self.features)
                self.cache = None
            except:
                print('Function', step_name, 'does not have a fit method or...')
                raise
                
    def transform(self, X):
        self.cache = X.copy()
        for step_name, step_function in self.steps.items():
            print('Attempting transform: ', step_name)
            try:
                self.cache = step_function.transform(self.cache)
            except KeyError as e:
                print('Problem with transform - missing value or column:', str(e))
            except:
                print('Function', step_name, 'does not have a transform method or..')
                raise
        return self.cache
    
    def initialize_data(self, features, targets):
        self.features = features
        self.targets = targets


class DataFrameScalar():
    '''
    
    Scaler Wrapper
    In order to maintain dataframes between scalars and provide column-specific scaling, 
    we'll need a to wrap the scalars in a simple class.

    This also yields the ability to have scalars only act on certain columns by normal 
    columns=[list of names] convention in pandas!

    **Currently only works with sklearn or scalars with fit, transform methods defined.
    '''
    def __init__(self, scaler=None, columns=None):
        self.scaler = scaler
        self.columns = columns
    
    def fit(self, X):
        if self.columns == None:
            self.scaler.fit(X)
        else:
            self.scaler.fit(X[self.columns])
    
    def transform(self, X):
        # Complete the transform, returns an array with no index
        Xcopy = X.copy()
        if self.columns == None:
            columns = Xcopy.columns
        else:
            columns = self.columns
        ar_transform = self.scaler.transform(Xcopy[columns])    
        # Add data back to dataframe
        assert type(columns==list)
        for i, col_name in enumerate(columns):
            Xcopy[col_name] = ar_transform[:,i]
        return Xcopy


