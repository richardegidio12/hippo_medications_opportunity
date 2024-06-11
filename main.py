import tarfile
import os
import pandas as pd
import json
from pandasql import sqldf


def extract_tar_gz(file_path, file_output_path=None):
    tar = tarfile.open(file_path, 'r:gz')
    if file_output_path:
        if not os.path.exists(file_output_path):
            os.makedirs(file_output_path)
        tar.extractall(file_output_path)
    else:
        file_output_path = "./tar_gz_extraction"
        tar.extractall(file_output_path)
    tar.close()


def list_folder_content(file_path, file_extension_desired=None) -> list:
    if file_extension_desired:
        json_files = [files for files in os.listdir(file_path) if files.endswith(f'.{file_extension_desired}')]
    else:
        json_files = os.listdir(file_path)
    return json_files


def read_files_json(file_path, folder_list)-> pd.DataFrame:
    if len(folder_list) > 1:
        data = [json.load(open(f'{file_path}/{file}', 'r')) for file in folder_list]
        dict_full = [i for item in data for i in item]
        df = pd.DataFrame(dict_full)
        return df
    else:
        df = json.load(open(f'{file_path}/{folder_list[0]}', 'r'))
        df = pd.DataFrame(df)
        return df


def read_files_csv(file_path, folder_list)-> pd.DataFrame:
    if len(folder_list) > 1:
        df = pd.DataFrame()
        for csv_file in folder_list:
            df_temp = pd.read_csv(f'{file_path}/{csv_file}')
            df = pd.concat([df, df_temp], ignore_index=True)
    else:
        df = pd.read_csv(f'{file_path}/{folder_list[0]}')
    return df


def write_json(output_json_path, file_to_convert):
    with open(output_json_path, 'w') as f:
        json.dump(file_to_convert, f, indent=4)


if __name__ == "__main__":

    name_folder = 'topics'
    claims_folder = 'claims'
    pharmacies_folder = 'pharmacies'
    reverts_folder = 'reverts'
    data_path_file_name = "./data.tar.gz"

    extract_tar_gz(data_path_file_name ,name_folder)

    # claims
    path_claims = f'{name_folder}/{claims_folder}'
    folder_claim_list = list_folder_content(path_claims, 'json')
    df_claim = read_files_json(path_claims, folder_claim_list)

    # pharmacies
    path_pharmacies = f'{name_folder}/{pharmacies_folder}'
    folder_pharmacies_list = list_folder_content(path_pharmacies, 'csv')
    df_pharmacies = read_files_csv(path_pharmacies, folder_pharmacies_list)

    # reverts
    path_reverts = f'{name_folder}/{reverts_folder}'
    folder_reverts_list = list_folder_content(path_reverts, 'json')
    df_reverts = read_files_json(path_reverts, folder_reverts_list)

    # Calculate metrics for some dimensions (npi, ndc)
    df_metrics_file_name = "json_df_counts.json"
    metrics = """
    SELECT c.npi, 
            c.ndc, 
            count( c.id) as fills, 
            count( r.id) as reverted, 
            round(avg(c.price),2) as avg_price,
            round(sum(c.price),1) as total_price
    FROM df_claim as c LEFT JOIN df_reverts as r ON c.id = r.claim_id
    WHERE c.npi in (select npi from df_pharmacies)
    GROUP BY c.npi, c.ndc
    order by 2,6 desc,3 desc
    """
    df_metrics = sqldf(metrics, locals())
    json_df_metrics = df_metrics.to_dict('records')
    json_df_metrics = json.dumps(json_df_metrics, indent=4)
    write_json(f'./{name_folder}/{df_metrics_file_name}', json_df_metrics)


    # Make a recommendation for the top 2 Chain to be displayed for each Drug
    df_recom_file_name = "json_df_recom.json"
    recom = """
    SELECT ndc, chain, avg_price from (SELECT  c.ndc, 
            p.chain,
            round(avg(c.price),2) as avg_price,
            row_number() over ( partition by ndc order by avg(c.price) asc ) as chain_rank
    FROM df_claim as c JOIN df_pharmacies as p ON c.npi = p.npi
    WHERE c.npi in (select npi from df_pharmacies)
    GROUP BY  p.chain, c.ndc )
    WHERE chain_rank <=2
    """
    df_recom = sqldf(recom, locals())
    json_df_recom = []

    for ndc, group in df_recom.groupby('ndc'):
        chain_list = []
        for _, row in group.iterrows():
            chain_list.append({
                "name": row['chain'],
                "avg_price": row['avg_price']
            })
        json_df_recom.append({
            "ndc": ndc,
            "chain": chain_list
        })
    json_df_recom = json.dumps(json_df_recom, indent=4)
    write_json(f'./{name_folder}/{df_recom_file_name}', json_df_recom)


    # Understand Most common quantity prescribed for a given Drug
    df_common_file_name = "json_df_common.json"
    commom = """
    select distinct(quantity) as most_prescribed_quantity, ndc from (SELECT  npi,
            ndc, 
            quantity,
            dense_rank() over (partition by ndc order by quantity desc) as rank
    FROM df_claim
    WHERE npi in (select npi from df_pharmacies)
    group by ndc,npi)
    where rank <= 5
    """
    df_commom_qty = sqldf(commom, locals())
    df_commom = df_commom_qty.groupby('ndc')['most_prescribed_quantity'].apply(list).reset_index()
    output_df_common = df_commom.to_dict('records')
    json_df_common = json.dumps(output_df_common, indent=4)
    write_json(f'./{name_folder}/{df_common_file_name}', json_df_common)





