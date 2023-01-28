import pandas as pd
import numpy as np
from paths import DATA_PATH, SAVE_CSV_PATH


class Recommender:

    def __init__(self):
        self.load_data(DATA_PATH)

    def load_data(self, data_path):
        try:
            self.df = pd.read_csv(data_path)
        except:
            raise Exception('Error load data')

    def recommend_for_region(self, region, weights={'sanctions': 0.3, 
                                                    'import': 0.4, 
                                                    'growth': 0.3}) -> pd.DataFrame:
    
        region_df = self.df.query(f'region_id == {region}').copy()
        region_tnved_id_list = list(region_df.tnved_id.unique())

        mr_import_2021 = pd.DataFrame(region_df.query(f'region_id=={region} and direction == "И" and year==2021').\
                                                groupby('tnved_id')['price'].sum().sort_values(ascending=False))
        mr_import_2021.rename(columns={'price': 'total_import_2021'}, inplace=True)
        mr_import_2021.reset_index(inplace=True)

        mr_export_2021 = pd.DataFrame(region_df.query(f'region_id=={region} and direction == "Э" and year==2021').\
                                                groupby('tnved_id')['price'].sum().sort_values(ascending=False))
        mr_export_2021.rename(columns={'price': 'total_export_2021'}, inplace=True)
        mr_export_2021.reset_index(inplace=True)

        mr_import_sanctions_2021 = pd.DataFrame(region_df.\
                                                query(f'region_id=={region} and direction == "И" and sanction_id >= 0 and year==2021').\
                                                groupby('tnved_id')['price'].sum().sort_values(ascending=False))
        mr_import_sanctions_2021.rename(columns={'price': 'total_sanctions_import'}, inplace=True)
        mr_import_sanctions_2021.reset_index(inplace=True)
        mr_import_sanctions_2021['import_sanctions_rank'] = mr_import_sanctions_2021['total_sanctions_import'].rank(ascending=False)

        mr_import_2020 = pd.DataFrame(region_df.query(f'region_id=={region} and direction == "И" and year==2020').\
                                                groupby('tnved_id')['price'].sum().sort_values(ascending=False))
        mr_import_2020.rename(columns={'price': 'total_import_2020'}, inplace=True)
        mr_import_2020.reset_index(inplace=True)

        mr_export_2020 = pd.DataFrame(region_df.query(f'region_id=={region} and direction == "Э" and year==2020').\
                                                groupby('tnved_id')['price'].sum().sort_values(ascending=False))
        mr_export_2020.rename(columns={'price': 'total_export_2020'}, inplace=True)
        mr_export_2020.reset_index(inplace=True)

        mr_import_2019 = pd.DataFrame(region_df.query(f'region_id=={region} and direction == "И" and year==2019').\
                                                groupby('tnved_id')['price'].sum().sort_values(ascending=False))
        mr_import_2019.rename(columns={'price': 'total_import_2019'}, inplace=True)
        mr_import_2019.reset_index(inplace=True)

        mr_export_2019 = pd.DataFrame(region_df.query(f'region_id=={region} and direction == "Э" and year==2019').\
                                                groupby('tnved_id')['price'].sum().sort_values(ascending=False))
        mr_export_2019.rename(columns={'price': 'total_export_2019'}, inplace=True)
        mr_export_2019.reset_index(inplace=True)

        recommendations = self.df[self.df.tnved_id.isin(region_tnved_id_list)].copy()

        for data in [mr_import_2021, mr_import_2020, mr_import_2019, mr_export_2019,
                     mr_export_2020, mr_export_2021, mr_import_sanctions_2021]:

            self.recommendations = self.recommendations.merge(data, how='left', on='tnved_id')

        recommendations.import_sanctions_rank.fillna(recommendations.import_sanctions_rank.max() + 1, inplace=True)
        recommendations.fillna(0, inplace=True)

        recommendations['pure_import_2019'] = recommendations['total_import_2019'] - recommendations['total_export_2019']
        recommendations['pure_import_2020'] = recommendations['total_import_2020'] - recommendations['total_export_2020']
        recommendations['pure_import_2021'] = recommendations['total_import_2021'] - recommendations['total_export_2021']

        recommendations['pure_import_2021_rank'] = recommendations['pure_import_2021'].rank(ascending=False, method='dense')

        recommendations['pure_import_2020_2019_growth'] = recommendations['pure_import_2020'] / recommendations['pure_import_2019']
        recommendations['pure_import_2021_2020_growth'] = recommendations['pure_import_2021'] / recommendations['pure_import_2020']
        recommendations.fillna(0, inplace=True)

        recommendations.loc[recommendations.pure_import_2020_2019_growth == np.inf, 'pure_import_2020_2019_growth'] = 0
        recommendations.loc[recommendations.pure_import_2021_2020_growth == np.inf, 'pure_import_2021_2020_growth'] = 0
        recommendations.loc[recommendations.pure_import_2020_2019_growth == -np.inf, 'pure_import_2020_2019_growth'] = -100
        recommendations.loc[recommendations.pure_import_2021_2020_growth == -np.inf, 'pure_import_2021_2020_growth'] = -100
        recommendations['average_pure_import_growth_percent'] = (recommendations['pure_import_2020_2019_growth'] + recommendations['pure_import_2021_2020_growth']) * 50

        recommendations['growth_rank'] = recommendations['average_pure_import_growth_percent'].rank(ascending=False)

        recommendations['growth_rank_norm'] = recommendations['growth_rank'] / recommendations['growth_rank'].max()
        recommendations['pure_import_2021_rank_norm'] = recommendations['pure_import_2021_rank'] / recommendations['pure_import_2021_rank'].max()
        recommendations['import_sanctions_rank_norm'] = recommendations['import_sanctions_rank'] / recommendations['import_sanctions_rank'].max()

        recommendations['final_rank'] = recommendations['growth_rank_norm'] * weights['growth'] + recommendations['pure_import_2021_rank_norm'] * weights['import'] + recommendations['import_sanctions_rank_norm'] * weights['sanctions']

        recommendations['region_id'] = region

        return recommendations[['tnved_id', 'region_id', 'tnved_name', 'tnved_code',
                                'growth_rank_norm', 'pure_import_2021_rank_norm', 
                                'import_sanctions_rank_norm', 'final_rank']].sort_values('final_rank').head(10)
    
    def recommend_for_all_regions(self, weights={'sanctions': 0.3, 
                                                 'import': 0.4, 
                                                 'growth': 0.3}) -> pd.DataFrame:
        for idx, region in enumerate(list(self.df.region_id.unique()), 1):
            if idx == 1:
                recommendations = self.recommend_for_region(self.df, region)
            else:
                recommendations = pd.concat([recommendations, self.recommend_for_region(self.df, region)])
        
        recommendations.reset_index(drop=True, inplace=True)
        recommendations['recommendation_id'] = list(range(1, 851))
        
        return recommendations


if __name__ == '__main__':
    recommender = Recommender()
    recommendations = recommender.recommend_for_all_regions()
    recommendations[['recommendation_id', 
                     'tnved_id', 
                     'region_id']].to_csv(SAVE_CSV_PATH + 'customs_recommendation.csv', index=False)

