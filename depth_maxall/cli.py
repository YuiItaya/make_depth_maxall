import argparse
import multiprocessing
import sys

from .errors import InputDataError
from .gui import launch_gui
from .io_utils import load_yaml_config
from .processing import run_legacy_pipeline, run_pipeline
from .utils import install_tkinter_shutdown_warning_filter

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description='浸水深又は浸水継続時間の最大包絡シェープファイルを作成します。'
    )
    parser.add_argument(
        'config',
        nargs='?',
        help='YAML設定ファイル。省略時はGUIを起動します。',
    )
    parser.add_argument(
        '--config',
        dest='config_option',
        help='YAML設定ファイル。',
    )
    parser.add_argument(
        '--gui',
        action='store_true',
        help='GUIを起動します。省略時もGUIを起動します。',
    )
    parser.add_argument(
        '--legacy',
        action='store_true',
        help='従来どおり ./shp を入力として処理します。',
    )
    return parser.parse_args(argv)

def main(argv=None):
    multiprocessing.freeze_support()
    install_tkinter_shutdown_warning_filter()

    args = parse_args(argv)

    try:
        config_path = args.config_option or args.config
        if config_path:
            run_pipeline(load_yaml_config(config_path))
        elif args.legacy:
            run_legacy_pipeline()
        else:
            launch_gui()
    except InputDataError as e:
        sys.exit(str(e))


if __name__ == "__main__":
    main()
