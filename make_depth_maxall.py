from depth_maxall.cli import main, parse_args
from depth_maxall.errors import InputDataError
from depth_maxall.gui import launch_gui
from depth_maxall.processing import run_legacy_pipeline, run_pipeline
from depth_maxall.validation import normalize_value_mapping, validate_value_column


if __name__ == "__main__":
    main()
