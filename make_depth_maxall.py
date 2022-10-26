# coding: utf_8

import os
import shutil
import sys
import time
import glob
from pathlib import Path

import pandas as pd
import geopandas as gpd

def main():
    start = time.time()

    #処理に用いるシェープファイルの場所
    INPUT_PATH = Path('./shp')
    if os.path.exists(INPUT_PATH) == True:
        shp = []
        for x in INPUT_PATH.glob('*.shp'):
            shp.append(x)
        else:
            if not shp:
                sys.exit('shpフォルダが空です。フォルダ内に浸水深シェープファイルを配置してください。')
    else:
        os.makedirs(INPUT_PATH)
        sys.exit('shpフォルダ内に浸水深シェープファイルを配置してください。')

    #処理に用いるシェープファイルの場所（水害リスクの山地等）
    EX_INPUT_PATH = Path(os.path.join(INPUT_PATH,'ex'))
    ex = []
    if os.path.exists(EX_INPUT_PATH) == True:
        for x in EX_INPUT_PATH.glob('*.shp'):
            ex.append(x)
    if ex:
        print('エクストラファイルの存在を確認しました。')

    #シェープファイルを浸水ランク別に分けた中間ファイル保存先
    SPLIT_PATH = Path('./split')
    shutil.rmtree(SPLIT_PATH)
    os.makedirs(SPLIT_PATH, exist_ok=True)

    #シェープファイルを浸水ランク別に分けた中間ファイル保存先（水害リスクの山地等）
    EX_SPLIT_PATH = Path(os.path.join(SPLIT_PATH,'ex'))
    if ex:
        os.makedirs(EX_SPLIT_PATH, exist_ok=True)
    else:
        if os.path.exists(EX_SPLIT_PATH) == True:
            shutil.rmtree(EX_SPLIT_PATH)

    #処理に用いる空のシェープファイルの保存先
    MATERIAL_PATH = Path('./material')

    #浸水ランク毎に結合した中間ファイル保存先
    RANK_PATH = Path('./rank')
    if os.path.exists(RANK_PATH) == True:
        shutil.rmtree(RANK_PATH)
        shutil.copytree(MATERIAL_PATH, RANK_PATH)
    else:
        shutil.copytree(MATERIAL_PATH, RANK_PATH)

    #浸水ランク毎に結合した中間ファイル保存先（水害リスクの山地等）
    EX_RANK_PATH = Path(os.path.join(RANK_PATH,'ex'))
    if ex:
        if os.path.exists(EX_RANK_PATH) == True:
            shutil.rmtree(EX_RANK_PATH)
            shutil.copytree(MATERIAL_PATH, EX_RANK_PATH)
        else:
            shutil.copytree(MATERIAL_PATH, EX_RANK_PATH)
    else:
        if os.path.exists(EX_RANK_PATH) == True:
            shutil.rmtree(EX_RANK_PATH)

    #出力先
    OUTPUT_PATH = Path('./output')
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    #想定される浸水深ランク一覧
    RANK_Existence = [1,2,3,4,5,6]

    #JGD2011(epsg:6668)
    dst_proj = 6668

    print('1/4_全シェープファイルを浸水ランク毎に分解中・・・')
    for depth_shp in INPUT_PATH.glob('*.shp'):
        depth_gpd = gpd.read_file(depth_shp, encoding='cp932')
        depth_gpd = depth_gpd.loc[:,["value","geometry"]].copy().to_crs(epsg = dst_proj)
        depth_name = str(depth_shp)[4:-4]    
        for value,group in depth_gpd.groupby('value'):
            group.to_file(filename = './split/{0}_{1}.shp'.format(depth_name,value),driver = "ESRI Shapefile",encoding = "shift-jis")

    #エクストラファイルが存在した場合の追加処理(1/3)
    if ex:
        for depth_shp in EX_INPUT_PATH.glob('*.shp'):
            depth_gpd = gpd.read_file(depth_shp, encoding='cp932')
            depth_gpd = depth_gpd.loc[:,["value","geometry"]].copy().to_crs(epsg = dst_proj)
            depth_name = str(depth_shp)[7:-4]    
            for value,group in depth_gpd.groupby('value'):
                group.to_file(filename = './split/ex/{0}_{1}.shp'.format(depth_name,value),driver = "ESRI Shapefile",encoding = "shift-jis")

    print('2/4_同一浸水ランクを結合します。')
    RANK = []
    for x in glob.glob(str(SPLIT_PATH) +  '/*_*.shp'):
        rename = str(x)[-5:-4]
        RANK.append(rename)
    else:
        RANK_set = sorted(set(RANK))

    for value in RANK_Existence:
        if str(value) in RANK_set:
            print("    " + f"RANK{value}をディゾルブ中・・・")
            RANKX_gpd = gpd.read_file(str(RANK_PATH) + '/Rank_' + str(value) + '.shp', encoding = 'cp932')
            for rank_x in glob.glob(str(SPLIT_PATH) + '/*_' + str(value) + '.shp'):
                rank_x_gpd = gpd.read_file(rank_x, encoding = 'cp932')
                RANKX_gpd_bool = RANKX_gpd.empty
                if RANKX_gpd_bool == True:
                    RANKX_gpd = rank_x_gpd.copy()
                else:
                    RANKX_gpd = pd.concat([RANKX_gpd,rank_x_gpd]).pipe(gpd.GeoDataFrame)
            else:
                RANKX_gpd = RANKX_gpd.dissolve()
                RANKX_gpd.to_file(filename = str(RANK_PATH) + '/Rank_' + str(value) + '.shp' ,driver = "ESRI Shapefile", encoding = "shift-jis")
        else:
            for p in glob.glob(str(RANK_PATH) + '/*_' + str(value)  + '.*'):
                os.remove(p)
            else:
                print("    " +f"RANK{value}は存在しませんでした。")

    #エクストラファイルが存在した場合の追加処理(2/3)
    if ex:
        print("    " +'エクストラファイルを処理します。')
        EX_RANK = []
        for x in glob.glob(str(EX_SPLIT_PATH) +  '/*_*.shp'):
            rename = str(x)[-5:-4]
            EX_RANK.append(rename)
        else:
            EX_RANK_set = sorted(set(EX_RANK))

        for value in RANK_Existence:
            if str(value) in EX_RANK_set:
                print("    " + f"RANK{value}をディゾルブ中・・・")
                RANKX_gpd = gpd.read_file(str(EX_RANK_PATH) + '/Rank_' + str(value) + '.shp', encoding = 'cp932')
                for rank_x in glob.glob(str(EX_SPLIT_PATH) + '/*_' + str(value) + '.shp'):
                    rank_x_gpd = gpd.read_file(rank_x, encoding = 'cp932')
                    RANKX_gpd_bool = RANKX_gpd.empty
                    if RANKX_gpd_bool == True:
                        RANKX_gpd = rank_x_gpd.copy()
                    else:
                        RANKX_gpd = pd.concat([RANKX_gpd,rank_x_gpd]).pipe(gpd.GeoDataFrame)
                else:
                    RANKX_gpd = RANKX_gpd.dissolve()
                    RANKX_gpd.to_file(filename = str(EX_RANK_PATH) + '/Rank_' + str(value) + '.shp' ,driver = "ESRI Shapefile", encoding = "shift-jis")
            else:
                for p in glob.glob(str(EX_RANK_PATH) + '/*_' + str(value)  + '.*'):
                    os.remove(p)
                else:
                    print("    " +f"RANK{value}は存在しませんでした。")

    print('3/4_浸水ランク間の重なりを判定し、重複する浅い浸水ランクを削除します。')
    for count, value in enumerate(reversed(RANK_set)):
        if count == 0:
            print("    " +f"RANK{value}をコピー中・・・")
            RANK_higher_gpd = gpd.read_file(str(RANK_PATH) + '/Rank_' + str(value) + '.shp', encoding = 'cp932')
        elif count == 1:
            print("    " +f"RANK{value}をユニオン中・・・")
            RANKX_gpd = gpd.read_file(str(RANK_PATH) + '/Rank_' + str(value) + '.shp', encoding = 'cp932').rename(columns={"value":f"value_{value}"})       
            RANK_higher_gpd = gpd.overlay(RANKX_gpd,RANK_higher_gpd, how = 'union').fillna(0)
            RANK_higher_gpd.loc[RANK_higher_gpd['value'] == 0,'value'] = int(f'{value}')
            RANK_higher_gpd = RANK_higher_gpd.dissolve(by = 'value').reset_index()
            del RANK_higher_gpd[f'value_{value}']
            RANK_higher_gpd_copy = RANK_higher_gpd.copy().reset_index(drop = True)
            dissolve_gpd = RANK_higher_gpd.dissolve().reset_index(drop = True)
            dissolve_gpd['value'] = int(99)
        else:
            print("    " +f"RANK{value}をユニオン中・・・")
            RANKX_gpd = gpd.read_file(str(RANK_PATH) + '/Rank_' + str(value) + '.shp', encoding = 'cp932').rename(columns={"value":f"value_{value}"})
            RANK_higher_gpd = gpd.overlay(dissolve_gpd, RANKX_gpd, how = 'union').fillna(0)
            RANK_higher_gpd.loc[RANK_higher_gpd['value'] == 0,'value'] = int(f'{value}')
            del RANK_higher_gpd[f'value_{value}']
            RANK_higher_gpd = RANK_higher_gpd.dissolve(by = 'value').reset_index()
            dissolve_gpd = RANK_higher_gpd.dissolve().reset_index(drop = True)
            dissolve_gpd['value'] = int(99)
            RANK_higher_gpd = RANK_higher_gpd.query('not value == 99')
            RANK_higher_gpd_copy = pd.concat([RANK_higher_gpd_copy,RANK_higher_gpd]).pipe(gpd.GeoDataFrame).reset_index(drop = True)
        
    #エクストラファイルが存在した場合の追加処理(3/3)
    if ex:
        print("    " +'エクストラファイルを処理します。')
        ALL_EX_RANK = []
        
        for x in glob.glob(str(EX_RANK_PATH) +  '/*_*.shp'):
            ALL_EX_RANK.append(gpd.read_file(x, encoding='cp932'))
        else:
            dissolve_gpd = RANK_higher_gpd_copy.dissolve().reset_index(drop = True)
            dissolve_gpd['value'] = int(99) 
            EX_GPD = pd.concat(ALL_EX_RANK).pipe(gpd.GeoDataFrame).reset_index(drop = True)
            EX_GPD_dis = EX_GPD.dissolve().reset_index(drop = True)
            EX_GPD_dis = EX_GPD_dis.rename(columns = {'value' : 'ex_value'})
            EX_GPD_dis['ex_value'] = int(98)          
            EX_GPD_dis = gpd.overlay(dissolve_gpd, EX_GPD_dis, how = 'union').fillna(0)  
            EX_GPD_dis = EX_GPD_dis.query('not value == 99').reset_index(drop = True)
            del EX_GPD_dis['value']
            EX_GPD = gpd.overlay(EX_GPD_dis, EX_GPD, how = 'union').fillna(0)
            EX_GPD = EX_GPD.query('not ex_value == 0').reset_index(drop = True)

            #誤差により生成される可能性のあるvalue=0を削除
            EX_GPD = EX_GPD.query('not value == 0').reset_index(drop = True)
            
            RANK_higher_gpd_copy = pd.concat([RANK_higher_gpd_copy,EX_GPD]).pipe(gpd.GeoDataFrame).reset_index(drop = True)
            RANK_higher_gpd_copy = RANK_higher_gpd_copy.dissolve(by = 'value').reset_index()
            del RANK_higher_gpd_copy['ex_value']

    print('4/4_シェープファイル出力中・・・')
    RANK_higher_gpd_copy.to_file(filename = str(OUTPUT_PATH) + '/depth_MAXALL.shp' ,driver = "ESRI Shapefile", encoding = "shift-jis")
    print('完了しました。')
    end = time.time()
    elapsed_time = end - start
    print(f"経過時間：{elapsed_time}秒")


if __name__ == "__main__":
    main()