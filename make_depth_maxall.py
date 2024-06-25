import shutil
import sys
import time
import pandas as pd
import geopandas as gpd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

JGD2011 = 6668
RANK_EXISTENCE = [1, 2, 3, 4, 5, 6, 7]

INPUT_PATH = Path('./shp')
OUTPUT_PATH = Path('./output')
SPLIT_PATH = Path('./split')
RANK_PATH = Path('./rank')
EXTRA_PATH = INPUT_PATH / 'ex'
EXTRA_SPLIT_PATH = SPLIT_PATH / 'ex'
EXTRA_RANK_PATH = RANK_PATH / 'ex'


def process_depth_shp(depth_shp, is_extra=False):
    depth_gpd = gpd.read_file(depth_shp, encoding='shift-jis')

    if "value" not in depth_gpd.columns and "rank" in depth_gpd.columns:
        depth_gpd = depth_gpd.rename(columns={"rank": "value"})
   
    depth_gpd["value"] = depth_gpd["value"].astype(int)
    depth_gpd = depth_gpd.loc[:, ["value", "geometry"]].copy().to_crs(epsg=JGD2011)
    depth_name = str(depth_shp)[4:-4] if not is_extra else str(depth_shp)[7:-4]

    for value, group in depth_gpd.groupby('value'):
        prefix = 'ex/' if is_extra else ''
        group.to_file(filename=f'./split/{prefix}{depth_name}_{value}.gpkg', driver="GPKG", encoding="shift-jis")


def format_elapsed_time(start, end):
    """
    スクリプトの処理時間を計算します。
    """
    elapsed_time = end - start
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"経過時間：{int(hours):02d}時間{int(minutes):02d}分{seconds:.2f}秒"


def create_directory(path: Path, clean=False):
    """
    指定したパスにディレクトリを作成します。cleanがTrueの場合、既存のディレクトリを削除してから新しく作成します。
    """
    if clean and path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def process_shapefiles(EX):
    print('1/4_全シェープファイルをランク毎に分解中・・・')
    # 並列処理の実行
    with ProcessPoolExecutor() as executor:
        futures = []

        for depth_shp in INPUT_PATH.glob('*.shp'):
            futures.append(executor.submit(process_depth_shp, depth_shp))

        if EX:
            for depth_shp in EXTRA_PATH.glob('*.shp'):
                futures.append(executor.submit(process_depth_shp, depth_shp, is_extra=True))

        # すべての並列タスクが完了するのを待つ
        for future in futures:
            future.result()


def process_ranked_data(EX):
    print('2/4_同一ランクを結合します。')
    RANK_set = sorted({x.stem[-1] for x in SPLIT_PATH.glob('*_*.gpkg')})

    for value in RANK_EXISTENCE:
        if str(value) in RANK_set:
            print("    " + f"RANK{value}をディゾルブ中・・・")
            RANKX_gpd = gpd.GeoDataFrame()
            for rank_x in SPLIT_PATH.glob(f'*_{value}.gpkg'):
                rank_x_gpd = gpd.read_file(rank_x, encoding='shift-jis')
                RANKX_gpd = pd.concat([RANKX_gpd, rank_x_gpd]).pipe(gpd.GeoDataFrame)
            else:
                RANKX_gpd["value"] = RANKX_gpd["value"].astype(int)
                RANKX_gpd = RANKX_gpd.dissolve()
                gpkg_file = RANK_PATH / f'Rank_{value}.gpkg'
                RANKX_gpd.to_file(filename=gpkg_file, driver="GPKG", encoding="shift-jis")

        else:
            for p in RANK_PATH.glob(f'*_{value}.*'):
                p.unlink()
            else:
                print("    " + f"RANK{value}は存在しませんでした。")

    if EX:
        process_extra_files()

    return RANK_set


def process_extra_files():
    print("    " + 'エクストラファイルを処理します。')
    EX_RANK = [x.stem[-1] for x in EXTRA_SPLIT_PATH.glob('*_*.gpkg')]
    EX_RANK_set = sorted(set(EX_RANK))

    for value in RANK_EXISTENCE:
        if str(value) in EX_RANK_set:
            print("    " + f"RANK{value}をディゾルブ中・・・")
            RANKX_gpd = gpd.GeoDataFrame()
            for rank_x in EXTRA_SPLIT_PATH.glob(f'*_{value}.gpkg'):
                rank_x_gpd = gpd.read_file(rank_x, encoding='shift-jis')
                RANKX_gpd = pd.concat([RANKX_gpd, rank_x_gpd]).pipe(gpd.GeoDataFrame)
            else:
                RANKX_gpd["value"] = RANKX_gpd["value"].astype(int)
                RANKX_gpd = RANKX_gpd.dissolve()
                gpkg_file = EXTRA_RANK_PATH / f'Rank_{value}.gpkg'
                RANKX_gpd.to_file(filename=gpkg_file, driver="GPKG", encoding="shift-jis")


def generate_final_output(EX, RANK_set):
    print('3/4_ランク間の重なりを判定し、重複する低ランクを削除します。')
    for count, value in enumerate(reversed(RANK_set)):
        if count == 0:
            print("    " + f"RANK{value}をコピー中・・・")
            RANK_higher_gpd = gpd.read_file(str(RANK_PATH) + '/Rank_' + str(value) + '.gpkg', encoding='shift-jis')
        elif count == 1:
            print("    " + f"RANK{value}をユニオン中・・・")
            RANKX_gpd = gpd.read_file(str(RANK_PATH) + '/Rank_' + str(value) + '.gpkg', encoding='shift-jis').rename(columns={"value": f"value_{value}"})
            RANK_higher_gpd = gpd.overlay(RANKX_gpd, RANK_higher_gpd, how='union').fillna(0)
            RANK_higher_gpd["value"] = RANK_higher_gpd["value"].astype(int)
            RANK_higher_gpd.loc[RANK_higher_gpd['value'] == 0, 'value'] = int(f'{value}')
            RANK_higher_gpd = RANK_higher_gpd.dissolve(by='value').reset_index()
            del RANK_higher_gpd[f'value_{value}']
            RANK_higher_gpd_copy = RANK_higher_gpd.copy().reset_index(drop=True)
            dissolve_gpd = RANK_higher_gpd.dissolve().reset_index(drop=True)
            dissolve_gpd['value'] = int(99)
        else:
            print("    " + f"RANK{value}をユニオン中・・・")
            RANKX_gpd = gpd.read_file(str(RANK_PATH) + '/Rank_' + str(value) + '.gpkg', encoding='shift-jis').rename(columns={"value": f"value_{value}"})
            RANK_higher_gpd = gpd.overlay(dissolve_gpd, RANKX_gpd, how='union').fillna(0)
            RANK_higher_gpd["value"] = RANK_higher_gpd["value"].astype(int)
            RANK_higher_gpd.loc[RANK_higher_gpd['value'] == 0, 'value'] = int(f'{value}')
            del RANK_higher_gpd[f'value_{value}']
            RANK_higher_gpd = RANK_higher_gpd.dissolve(by='value').reset_index()
            dissolve_gpd = RANK_higher_gpd.dissolve().reset_index(drop=True)
            dissolve_gpd['value'] = int(99)
            RANK_higher_gpd = RANK_higher_gpd.query('not value == 99')
            RANK_higher_gpd_copy = pd.concat([RANK_higher_gpd_copy, RANK_higher_gpd]).pipe(gpd.GeoDataFrame).reset_index(drop=True)

    # エクストラファイルが存在した場合の追加処理(3/3)
    if EX:
        print("    " + 'エクストラファイルを処理します。')
        ALL_EX_RANK = [gpd.read_file(x, encoding='shift-jis') for x in EXTRA_RANK_PATH.glob('*_*.gpkg')]

        dissolve_gpd = RANK_higher_gpd_copy.dissolve().reset_index(drop=True)
        dissolve_gpd['value'] = int(99)

        EX_GPD = pd.concat(ALL_EX_RANK).pipe(gpd.GeoDataFrame).reset_index(drop=True)
        EX_GPD["value"] = EX_GPD["value"].astype(int)
        EX_GPD_dis = EX_GPD.dissolve().reset_index(drop=True)
        EX_GPD_dis = EX_GPD_dis.rename(columns={'value': 'ex_value'})
        EX_GPD_dis['ex_value'] = int(98)
        EX_GPD_dis = gpd.overlay(dissolve_gpd, EX_GPD_dis, how='union').fillna(0)
        EX_GPD_dis = EX_GPD_dis.query('not value == 99').reset_index(drop=True)
        del EX_GPD_dis['value']

        EX_GPD = gpd.overlay(EX_GPD_dis, EX_GPD, how='union').fillna(0)
        EX_GPD["value"] = EX_GPD["value"].astype(int)
        EX_GPD = EX_GPD.query('not ex_value == 0').reset_index(drop=True)
        # 誤差により生成される可能性のあるvalue=0を削除
        EX_GPD = EX_GPD.query('not value == 0').reset_index(drop=True)

        RANK_higher_gpd_copy = pd.concat([RANK_higher_gpd_copy, EX_GPD]).pipe(gpd.GeoDataFrame).reset_index(drop=True)
        RANK_higher_gpd_copy = RANK_higher_gpd_copy.dissolve(by='value').reset_index()
        del RANK_higher_gpd_copy['ex_value']

    print('4/4_シェープファイル出力中・・・')
    RANK_higher_gpd_copy["value"] = RANK_higher_gpd_copy["value"].astype(int)
    RANK_higher_gpd_copy.to_file(filename=str(OUTPUT_PATH) + '/output.shp', driver="ESRI Shapefile", encoding="utf-8")


def main():
    start = time.time()

    # 処理に用いるシェープファイルの場所
    if INPUT_PATH.exists():
        shp = list(INPUT_PATH.glob('*.shp'))
        if not shp:
            sys.exit('shpフォルダが空です。フォルダ内にシェープファイルを配置してください。')
    else:
        create_directory(INPUT_PATH, clean=False)
        sys.exit('shpフォルダ内にシェープファイルを配置してください。')

    create_directory(SPLIT_PATH, clean=True) # シェープファイルをランク別に分けた中間ファイル保存先  
    create_directory(RANK_PATH, clean=True) # ランク毎に結合した中間ファイル保存先        
    create_directory(OUTPUT_PATH, clean=True) # 出力先
 
    # 処理に用いるシェープファイルの場所（中小河川の山地等表示順位の低いもの）
    EX = list(EXTRA_PATH.glob('*.shp')) if EXTRA_PATH.exists() else []
    if EX:
        print('エクストラファイルの存在を確認しました。')
        create_directory(EXTRA_SPLIT_PATH, clean=True) # シェープファイルをランク別に分けた中間ファイル保存先（中小河川の山地等表示順位の低いもの）
        create_directory(EXTRA_RANK_PATH, clean=True) # ランク毎に結合した中間ファイル保存先（中小河川の山地等表示順位の低いもの）
    
    # 各種処理の実行
    process_shapefiles(EX)
    RANK_set = process_ranked_data(EX)
    generate_final_output(EX, RANK_set)

    print('完了しました。')
    end = time.time()
    print(format_elapsed_time(start, end))


if __name__ == "__main__":
    main()