import pandas as pd
import os
import re
from paths import CUSTOM_DATA_PATH, TNVED_CODES_PATH, SANCTIONS_PATH, SAVE_CSV_PATH
from constants import EDIZM_CODES_DICT, COUNTRIES, COUNTRIES_BLOCK


class Preprocessor:
    
    def __init__(self):
        self.load_data()
        self.preprocess_data()
        self.prepare_to_save_data()
    
    def load_data(self):
        
        # load custom_data
        files_paths_list = []
        
        for address, dirs, files in os.walk(CUSTOM_DATA_PATH):
            for name in files:
                if '.csv' in name:
                    files_paths_list.append(os.path.join(address, name))
                    

        for idx, data in enumerate(files_paths_list):
            if idx == 0:
                self.all_data = pd.read_csv(data, sep='\t', dtype='str')
            else:
                self.all_data = pd.concat([self.all_data, 
                                           pd.read_csv(data, sep='\t', dtype='str')], 
                                           ignore_index=True)

        
        # load tnved_codes

        self.tnved_codes = pd.read_csv(TNVED_CODES_PATH, 
                                        delimiter=',', 
                                        dtype='object')
        
        # load sanctions
        sanctions_paths_list = []
        for address, dirs, files in os.walk(SANCTIONS_PATH):
            for name in files:
                if '.xlsx' in name:
                    sanctions_paths_list.append(os.path.join(address, name))
                    
        for idx, data in enumerate(sanctions_paths_list):
            if idx == 0:
                sanctions_data_import = pd.read_excel(data, 
                                                      usecols=[0],
                                                      names=['tnved'],
                                                      sheet_name=0,
                                                      dtype='object')
                sanctions_data_import['country_name'] = data.split('/')[-1].partition('_санкции')[0]

                try:
                    sanctions_data_export = pd.read_excel(data, 
                                                          usecols=[0],
                                                          sheet_name=1,
                                                          names=['tnved'],
                                                          dtype='string')

                    sanctions_data_export['country_name'] = data.split('/')[-1].partition('_санкции')[0]
                except:
                    pass
            else:
                new_data = pd.read_excel(data, 
                                         usecols=[0],
                                         sheet_name=0,
                                         names=['tnved'],
                                         dtype='string')
                new_data['country_name'] = data.split('/')[-1].partition('_санкции')[0]

                sanctions_data_import = pd.concat([sanctions_data_import, 
                                                   new_data],
                                                   ignore_index=True)
                try:
                    new_data = pd.read_excel(data, 
                                             usecols=[0],
                                             sheet_name=1,
                                             names=['tnved'],
                                             dtype='string')
                    new_data['country_name'] = data.split('/')[-1].partition('_санкции')[0]

                    sanctions_data_export = pd.concat([sanctions_data_export, 
                                                       new_data],
                                                       axis=0,
                                                       ignore_index=True)

                except:
                    pass
                    
            sanctions_data_export['direction'] = 'Э'
            sanctions_data_import['direction'] = 'И'
            
            self.sanctions_data = pd.concat([sanctions_data_import, sanctions_data_export], ignore_index=False)
            
    def preprocess_data(self):
        # all_data
        self.all_data.columns = ['direction', 'period', 'country_code', 'tnved', 
                                  'unit_code', 'price', 'volume', 'quantity',
                                  'r_source_name', 'fd_source_name'] 
        self.all_data.price = self.all_data.price.str.replace(',', '.').astype('float')
        self.all_data.volume = self.all_data.volume.str.replace(',', '.').astype('float')
        self.all_data.quantity = self.all_data.quantity.str.replace(',', '.').astype('float')
        self.all_data.period = pd.to_datetime(self.all_data.period, format='%m/%Y')
        self.all_data['country_code'].fillna('NNN', inplace=True)
        self.all_data['unit_code'].fillna('166', inplace=True)
        self.all_data.quantity = self.all_data.quantity.str.replace(',', '.').astype('float')
        self.all_data.loc[self.all_data.direction=='ЭК', 'direction'] = 'Э'
        self.all_data.loc[self.all_data.direction=='ИМ', 'direction'] = 'И'
         
        # federal district catalog
        all_federal_districts = list(self.all_data.fd_source_name.unique())
        prepared_federal_districts_list = [{'federal_district_id': idx, 
                                            'federal_district_code': district.split('-')[0].rstrip(), 
                                            'federal_district_name': district.split('-')[-1].lstrip().capitalize(),
                                            'fd_source_name': district}
                                            for idx, district in enumerate(all_federal_districts, 1)]
        self.federal_district_catalog  = pd.DataFrame(prepared_federal_districts_list)
            
        # region catalog
        all_regions = list(self.all_data.r_source_name.unique())
        prepared_regions_list = [{'region_id': idx, 
                                  'region_code': district.split('-')[0].rstrip(), 
                                  'region_name': district.partition('-')[-1].lstrip().capitalize(),
                                  'r_source_name': district}
                                 for idx, district in enumerate(all_regions, 1)]
        for idx, region in enumerate(prepared_regions_list):
            if 'Республика' in region['region_name'].title():
                region['region_name'] = region['region_name'].title()
            if 'Москва' in region['region_name'].title():
                region['region_name'] = 'Москва'
            if 'Санкт-Петербург' in region['region_name'].title():
                region['region_name'] = 'Санкт-Петербург'
            if 'Севастополь' in region['region_name'].title():
                region['region_name'] = 'Севастополь'
        self.region_catalog = pd.DataFrame(prepared_regions_list)
            
        # units
        all_units_in_df = list(self.all_data.unit_code.unique())
        prepared_units_list = [{'unit_id': idx, 
                                'unit_code': unit, 
                                'unit_name': EDIZM_CODES_DICT[unit]}
                               for idx, unit in enumerate(all_units_in_df, 1)]
        self.units = pd.DataFrame(prepared_units_list)
            
        # countries
        all_countries_in_df = list(self.all_data.country_code.unique())
        prepared_countries_list = [{'country_id': idx, 
                                    'country_name': COUNTRIES[country],
                                    'country_block': 'Европейский союз (ЕС)' if country in COUNTRIES_BLOCK else 'Нет',
                                    'country_code': country}
                                     for idx, country in enumerate(all_countries_in_df, 1)]
        self.countries = pd.DataFrame(prepared_countries_list)
            
        # tnved
        self.tnved_codes = self.tnved_codes.sort_values('tnved_code').reset_index(drop=True)
        self.tnved_codes = self.tnved_codes.reset_index()            
        self.tnved_codes.rename(columns={'index': 'tnved_id', 'fee': 'tnved_fee', 'tnved_code': 'tnved'}, 
                                inplace=True)
            
        # sanctions
        self.sanctions_data.fillna('дроп', inplace=True)
        self.sanctions_data.tnved = self.sanctions_data.tnved.astype('str')

        self.sanctions_data.tnved = self.sanctions_data.tnved.str.replace(' ', '').str.replace('из', '').                                    str.replace('и', ',').str.replace('[^0-9,]','').str.split(',')
        self.sanctions_data = self.sanctions_data.explode('tnved')

        self.sanctions_data = self.sanctions_data.loc[self.sanctions_data['tnved'] != '']

        self.sanctions_data = self.sanctions_data.reset_index(drop=True)

        ES_COUNTRIES_LIST = list(self.countries.                                 query('country_block == "Европейский союз (ЕС)"')['country_name'].unique())
        ES_COUNTRIES_LIST.remove('Европейский союз (ЕС)')
        ES_COUNTRIES_LIST = ' '.join(ES_COUNTRIES_LIST)

        self.sanctions_data.loc[self.sanctions_data.country_name == 'Европейский союз',
                                'country_name'] = ES_COUNTRIES_LIST
        self.sanctions_data.loc[self.sanctions_data.country_name=='Швейцария', 'country_name'] = 'Швейцария'

        self.sanctions_data.country_name = self.sanctions_data.country_name.str.split(' ')

        self.sanctions_data = self.sanctions_data.explode('country_name', ignore_index=True)

        self.sanctions_data = self.sanctions_data.sort_values('tnved')

        ALL_TNVED_STRING = ' '.join(list(self.tnved_codes.tnved.unique()))
        ALL_SANCTIONS_TNVED_LIST = list(self.sanctions_data.tnved.unique())

        sanctions_dict = {}
        for sanction_tnved in ALL_SANCTIONS_TNVED_LIST:
            if re.findall(f'\\b{sanction_tnved}\\w*', ALL_TNVED_STRING):
                sanctions_dict[sanction_tnved] = re.findall(f'\\b{sanction_tnved}\\w*', ALL_TNVED_STRING)

        def sanctions_prepare_tnved(row):
            row.tnved = sanctions_dict.get(row.tnved, row.tnved)
            if len(row.tnved) < 10:
                row.tnved = 'drop'
            return row

        self.sanctions_data = self.sanctions_data.apply(sanctions_prepare_tnved, axis=1)

        self.sanctions_data = self.sanctions_data[self.sanctions_data.tnved != 'drop'].                                                                        explode('tnved', ignore_index=True)

        self.sanctions_data.loc[self.sanctions_data.country_name == "США", 
                                'country_name'] = 'Соединенные Штаты Америки (США)'
            
    def prepare_to_save_data(self):

        # all_data
        self.all_data = self.all_data.merge(self.federal_district_catalog, on='fd_source_name', how='left')
        self.all_data = self.all_data.merge(self.region_catalog, on='r_source_name', how='left')
        self.all_data = self.all_data.merge(self.units, on='unit_code', how='left')
        self.all_data = self.all_data.merge(self.countries, on='country_code', how='left')
        self.all_data = self.all_data.merge(self.tnved_codes, on='tnved', how='left')

        self.all_data.dropna(inplace=True)

        self.all_data.tnved_id = self.all_data.tnved_id.astype('int')

        self.all_data.drop(columns=['fd_source_name', 'r_source_name'], inplace=True)

        self.all_data.reset_index(inplace=True, drop=True)

        # federal district
        self.federal_district_catalog.drop(columns=['fd_source_name'], inplace=True)

        # region
        self.region_catalog.drop(columns=['r_source_name'], inplace=True)
        self.region_catalog = self.region_catalog.merge(self.all_data[['region_id', 
                                                                       'federal_district_id']], 
                                                        on='region_id', 
                                                        how='left')

        self.region_catalog.drop_duplicates(inplace=True)

        self.region_catalog.reset_index(drop=True, inplace=True)

        # countries
        self.countries.drop(columns=['country_code'], inplace=True)

        # tnved
        self.tnved_codes.rename(columns={'tnved': 'tnved_code'}, inplace=True)

        # sanctions
        self.sanctions_data = self.sanctions_data.merge(self.countries[['country_name', 
                                                                        'country_id']], 
                                                        on='country_name', 
                                                        how='left')

        self.sanctions_data.rename(columns={'tnved': 'tnved_code'}, inplace=True)
        self.sanctions_data = self.sanctions_data.merge(self.tnved_codes[['tnved_code', 'tnved_id']], 
                                                        on='tnved_code', 
                                                        how='left')

        self.sanctions_data.dropna(inplace=True)
        self.sanctions_data.tnved_id = self.sanctions_data.tnved_id.astype('int')

        self.sanctions_data.drop(columns=['country_name', 'tnved_code'], inplace=True)

        self.sanctions_data.reset_index(inplace=True)

        self.sanctions_data.rename(columns={'index': 'sanction_id'}, inplace=True)

        # customs
        self.customs_data = self.all_data[['direction', 'period', 'price', 'volume', 
                                           'quantity', 'region_id', 'country_id', 'tnved_id', 'unit_id']].copy()

        self.customs_data.columns = ['direction', 'period', 'price', 'volume', 'quantity', 
                                     'custom_data_region_id', 'custom_data_country_id', 
                                     'custom_data_tnved_id', 'custom_data_unit_id']

        self.customs_data.custom_data_tnved_id = self.customs_data.custom_data_tnved_id.astype('int')

    def save_to_csv(self, save_path):

        self.federal_district_catalog.to_csv(save_path + 'customs_federaldistrict.csv', index=False)
        self.region_catalog.to_csv(save_path + 'customs_region.csv', index=False)
        self.units.to_csv(save_path + 'customs_unit.csv', index=False)
        self.countries.to_csv(save_path + 'customs_country.csv', index=False)
        self.tnved_codes.to_csv(save_path + 'customs_customtnvedcode.csv', index=False)
        self.sanctions_data.to_csv(save_path + 'customs_sanction.csv', index=False)
        self.customs_data.to_csv(save_path + 'customs_customdata.csv', index=False)


if __name__ == '__main__':
    preprocessor = Preprocessor()
    preprocessor.save_to_csv(SAVE_CSV_PATH)
