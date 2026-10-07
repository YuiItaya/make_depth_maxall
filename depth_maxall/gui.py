from pathlib import Path
import re

import pandas as pd

from .config import normalize_config
from .constants import (
    DEFAULT_OUTPUT_EPSG,
    DEFAULT_OUTPUT_FIELD,
    DEFAULT_OUTPUT_FILE,
    DEFAULT_VALUE_FIELD_CANDIDATES,
    OUTPUT_CRS_CHOICES,
    OUTPUT_CRS_EPSG_TO_LABEL,
    OUTPUT_CRS_LABEL_TO_EPSG,
    VALUE_RECLASS_ID_TO_LABEL,
    VALUE_RECLASS_LABEL_TO_ID,
    VALUE_RECLASS_TEMPLATE_CHOICES,
    VALUE_RECLASS_TEMPLATES,
)
from .errors import InputDataError
from .io_utils import (
    get_attribute_field_values,
    get_attribute_fields,
    get_attribute_unique_text_values,
    get_field_conversion_status,
    load_yaml_config,
    save_yaml_config,
)
from .map_selector import select_clip_bounds_on_map
from .processing import run_pipeline
from .utils import format_limited_values, sanitize_filename
from .validation import apply_value_reclass, normalize_fixed_value, normalize_value_mapping, normalize_value_reclass

def launch_gui():
    import contextlib
    import queue
    import threading
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    class QueueWriter:
        def __init__(self, output_queue):
            self.output_queue = output_queue

        def write(self, text):
            if text:
                self.output_queue.put(("log", text))

        def flush(self):
            pass

    root = tk.Tk()
    root.title("最大包絡シェープファイル作成")
    root.geometry("1180x900")
    root.minsize(1040, 820)

    input_items = []
    output_queue = queue.Queue()
    worker_thread = {"thread": None}

    dissolve_var = tk.BooleanVar(value=True)
    keep_intermediate_var = tk.BooleanVar(value=False)
    extra_var = tk.BooleanVar(value=False)
    fixed_rank_var = tk.BooleanVar(value=False)
    fixed_value_var = tk.StringVar()
    field_var = tk.StringVar()
    name_var = tk.StringVar()
    group_var = tk.StringVar()
    output_group_var = tk.StringVar()
    warning_var = tk.StringVar()
    output_path_var = tk.StringVar(value=str(DEFAULT_OUTPUT_FILE))
    output_field_var = tk.StringVar(value=DEFAULT_OUTPUT_FIELD)
    output_crs_var = tk.StringVar(value=OUTPUT_CRS_EPSG_TO_LABEL[DEFAULT_OUTPUT_EPSG])
    output_group_files_var = tk.BooleanVar(value=False)
    clip_vars = [tk.StringVar() for _ in range(4)]
    editor_index = {"value": None}

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    main_frame = ttk.Frame(root, padding=8)
    main_frame.grid(row=0, column=0, sticky="nsew")
    main_frame.columnconfigure(0, weight=1)
    main_frame.rowconfigure(0, weight=1)
    main_frame.rowconfigure(6, weight=1)

    tree = ttk.Treeview(
        main_frame,
        columns=("warning", "output_group", "group", "name", "field", "kind", "path"),
        show="headings",
        selectmode="extended",
        height=10,
    )
    tree.heading("warning", text="警告")
    tree.heading("output_group", text="河川出力グループ名")
    tree.heading("group", text="データグループ名")
    tree.heading("name", text="名前")
    tree.heading("field", text="フィールド")
    tree.heading("kind", text="区分")
    tree.heading("path", text="ファイル")
    tree.column("warning", width=45, stretch=False, anchor="center")
    tree.column("output_group", width=170, stretch=False)
    tree.column("group", width=170, stretch=False)
    tree.column("name", width=150, stretch=False)
    tree.column("field", width=120, stretch=False)
    tree.column("kind", width=70, stretch=False)
    tree.column("path", width=360, stretch=True)
    tree.tag_configure("warning", background="#fff4c2", foreground="#6b4f00")
    tree.tag_configure("caution", background="#edf2f7", foreground="#475569")
    tree.grid(row=0, column=0, sticky="nsew")

    tree_scroll = ttk.Scrollbar(main_frame, orient="vertical", command=tree.yview)
    tree_scroll.grid(row=0, column=1, sticky="ns")
    tree.configure(yscrollcommand=tree_scroll.set)

    button_frame = ttk.Frame(main_frame)
    button_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 8))

    editor = ttk.LabelFrame(main_frame, text="選択ファイル")
    editor.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 8))
    editor.columnconfigure(1, weight=1)
    editor.columnconfigure(3, weight=1)

    ttk.Label(editor, text="河川出力グループ名").grid(row=0, column=0, padx=6, pady=6, sticky="w")
    output_group_entry = ttk.Entry(editor, textvariable=output_group_var)
    output_group_entry.grid(row=0, column=1, padx=6, pady=6, sticky="ew")

    ttk.Label(editor, text="データグループ名").grid(row=0, column=2, padx=6, pady=6, sticky="w")
    group_entry = ttk.Entry(editor, textvariable=group_var)
    group_entry.grid(row=0, column=3, padx=6, pady=6, sticky="ew")

    ttk.Label(editor, text="名前").grid(row=1, column=0, padx=6, pady=6, sticky="w")
    name_entry = ttk.Entry(editor, textvariable=name_var)
    name_entry.grid(row=1, column=1, padx=6, pady=6, sticky="ew")

    ttk.Label(editor, text="フィールド").grid(row=1, column=2, padx=6, pady=6, sticky="w")
    field_combo = ttk.Combobox(editor, textvariable=field_var, state="readonly", width=20)
    field_combo.grid(row=1, column=3, padx=6, pady=6, sticky="ew")

    fixed_rank_check = ttk.Checkbutton(editor, text="固定rank", variable=fixed_rank_var)
    fixed_rank_check.grid(row=1, column=4, padx=(6, 2), pady=6, sticky="w")
    fixed_value_entry = ttk.Entry(editor, textvariable=fixed_value_var, width=6)
    fixed_value_entry.grid(row=1, column=5, padx=(0, 6), pady=6, sticky="w")

    extra_check = ttk.Checkbutton(editor, text="低優先", variable=extra_var)
    extra_check.grid(row=1, column=6, padx=6, pady=6, sticky="w")

    ttk.Label(editor, textvariable=warning_var, foreground="#475569").grid(
        row=2,
        column=0,
        columnspan=9,
        padx=6,
        pady=(0, 6),
        sticky="w",
    )

    options = ttk.LabelFrame(main_frame, text="入力データ処理")
    options.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(0, 8))
    options.columnconfigure(1, weight=1)

    ttk.Checkbutton(
        options,
        text="同一値を事前ディゾルブ",
        variable=dissolve_var,
    ).grid(row=0, column=0, padx=6, pady=6, sticky="w")
    ttk.Checkbutton(
        options,
        text="中間ファイルを残す",
        variable=keep_intermediate_var,
    ).grid(row=0, column=1, padx=12, pady=6, sticky="w")

    bounds_frame = ttk.LabelFrame(main_frame, text="処理範囲（全入力共通・任意）")
    bounds_frame.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(0, 8))
    bounds_frame.columnconfigure(9, weight=1)

    ttk.Label(
        bounds_frame,
        text="空欄の場合は全範囲を処理します。指定する場合は EPSG:6668（JGD2011）の経度・緯度で入力してください。",
    ).grid(row=0, column=0, columnspan=10, padx=6, pady=(6, 2), sticky="w")

    coordinate_labels = (
        ("左端 経度 minx", 0),
        ("下端 緯度 miny", 1),
        ("右端 経度 maxx", 2),
        ("上端 緯度 maxy", 3),
    )
    for index, (label, _) in enumerate(coordinate_labels):
        ttk.Label(bounds_frame, text=label).grid(
            row=1,
            column=index * 2,
            padx=(6 if index == 0 else 12, 3),
            pady=(4, 6),
            sticky="e",
        )
        ttk.Entry(bounds_frame, textvariable=clip_vars[index], width=14).grid(
            row=1,
            column=index * 2 + 1,
            padx=(0, 3),
            pady=(4, 6),
            sticky="w",
        )

    output_frame = ttk.LabelFrame(main_frame, text="出力設定")
    output_frame.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(0, 8))
    output_frame.columnconfigure(1, weight=1)
    output_frame.columnconfigure(5, weight=1)

    ttk.Label(output_frame, text="出力ファイル").grid(row=0, column=0, padx=6, pady=6, sticky="w")
    ttk.Entry(output_frame, textvariable=output_path_var).grid(row=0, column=1, padx=6, pady=6, sticky="ew")
    ttk.Button(output_frame, text="参照", command=lambda: choose_output_file()).grid(
        row=0,
        column=2,
        padx=(0, 12),
        pady=6,
        sticky="w",
    )

    ttk.Label(output_frame, text="出力フィールド").grid(row=0, column=3, padx=6, pady=6, sticky="w")
    ttk.Entry(output_frame, textvariable=output_field_var, width=12).grid(row=0, column=4, padx=6, pady=6, sticky="w")

    ttk.Label(output_frame, text="出力座標系").grid(row=0, column=5, padx=6, pady=6, sticky="e")
    ttk.Combobox(
        output_frame,
        textvariable=output_crs_var,
        values=[label for label, _epsg in OUTPUT_CRS_CHOICES],
        state="readonly",
        width=42,
    ).grid(row=0, column=6, padx=6, pady=6, sticky="ew")
    ttk.Checkbutton(
        output_frame,
        text="河川・グループ別出力も作成",
        variable=output_group_files_var,
    ).grid(row=1, column=0, columnspan=7, padx=6, pady=(0, 6), sticky="w")

    log_text = tk.Text(main_frame, height=12, wrap="word")
    log_text.grid(row=6, column=0, columnspan=2, sticky="nsew")
    log_text.configure(state="disabled")

    def log(message):
        log_text.configure(state="normal")
        log_text.insert("end", message)
        log_text.see("end")
        log_text.configure(state="disabled")

    def update_item_conversion_warning(item):
        item["warning"] = ""
        item["warning_message"] = ""
        item["warning_level"] = ""
        if not Path(item.get("path", "")).exists():
            item["warning"] = "▲"
            item["warning_message"] = "入力ファイルが存在しません。パスを確認するか、再度追加してください。"
            item["warning_level"] = "warning"
            item["missing_path"] = True
            return
        item["missing_path"] = False

        if item.get("fixed_value") is not None:
            return

        field = item.get("field")
        if not field:
            return

        try:
            status = get_field_conversion_status(
                item["path"],
                field,
                item.get("value_mapping"),
                item.get("value_reclass"),
            )
        except Exception as e:
            item["warning"] = "▲"
            item["warning_message"] = f"フィールド確認に失敗しました: {e}"
            item["warning_level"] = "warning"
            return

        if status.get("has_unmapped_values"):
            item["warning"] = "▲"
            item["warning_message"] = status.get("message", "")
            item["warning_level"] = "warning"
        elif status.get("has_caution"):
            item["warning"] = "△"
            item["warning_message"] = status.get("message", "")
            item["warning_level"] = "caution"

    def update_group_conversion_warnings(group):
        for item in input_items:
            if item.get("group") == group:
                update_item_conversion_warning(item)

    def refresh_tree():
        selected = tree.selection()
        selected_id = selected[0] if selected else None
        for row_id in tree.get_children():
            tree.delete(row_id)

        for index, item in enumerate(input_items):
            row_id = str(index)
            tree.insert(
                "",
                "end",
                iid=row_id,
                values=(
                    item.get("warning", ""),
                    item.get("output_group") or item.get("group", ""),
                    item.get("group", ""),
                    item.get("name", ""),
                    (
                        f"固定rank={item.get('fixed_value')}"
                        if item.get("fixed_value") is not None
                        else (
                            f"{item.get('field', '')} / 式:{VALUE_RECLASS_ID_TO_LABEL[item['value_reclass']['template']]}"
                            if item.get("value_reclass")
                            else item.get("field", "")
                        )
                    ),
                    "低優先" if item.get("is_extra") else "通常",
                    item.get("path", ""),
                ),
                tags=(item.get("warning_level"),) if item.get("warning_level") else (),
            )

        if selected_id and tree.exists(selected_id):
            tree.selection_set(selected_id)

    def selected_index():
        selected = tree.selection()
        if not selected:
            return None
        return int(selected[0])

    def selected_indices():
        return sorted(int(row_id) for row_id in tree.selection())

    def choose_default_field(fields):
        for candidate in DEFAULT_VALUE_FIELD_CANDIDATES:
            if candidate in fields:
                return candidate
        return fields[0] if fields else ""

    def make_unique_item_value(base_value, key):
        base_value = sanitize_filename(base_value)
        used_values = {item.get(key) for item in input_items}
        if base_value not in used_values:
            return base_value

        suffix = 2
        while True:
            candidate = sanitize_filename(f"{base_value}_{suffix}")
            if candidate not in used_values:
                return candidate
            suffix += 1

    def add_files():
        paths = filedialog.askopenfilenames(
            title="入力シェープファイルを選択",
            filetypes=(("Shapefile", "*.shp"), ("All files", "*.*")),
        )
        if not paths:
            return

        failed_files = []
        for path in paths:
            try:
                fields = get_attribute_fields(path)
            except Exception as e:
                failed_files.append((path, e))
                continue

            path_stem = sanitize_filename(Path(path).stem)
            unique_name = make_unique_item_value(path_stem, "name")
            unique_group = make_unique_item_value(path_stem, "group")
            item = {
                "path": str(Path(path)),
                "field": choose_default_field(fields),
                "fixed_value": None,
                "is_extra": False,
                "name": unique_name,
                "group": unique_group,
                "group_user_set": False,
                "output_group": unique_group,
                "output_group_user_set": False,
                "fields": fields,
            }
            update_item_conversion_warning(item)
            input_items.append(item)

        refresh_tree()

        if failed_files:
            log("入力ファイルの読込に失敗しました。\n")
            for path, error in failed_files:
                log(f"  {path}\n    {error}\n")
            messagebox.showerror(
                "読込エラー",
                f"{len(failed_files)}件のファイルを読み込めませんでした。"
                "詳細は画面下部のログを確認してください。",
            )

    def remove_selected():
        indices = selected_indices()
        if not indices:
            return

        for index in reversed(indices):
            del input_items[index]

        if editor_index["value"] in indices:
            editor_index["value"] = None
            field_combo.configure(values=())
            field_var.set("")
            name_var.set("")
            group_var.set("")
            output_group_var.set("")
            fixed_rank_var.set(False)
            fixed_value_var.set("")
            warning_var.set("")
            extra_var.set(False)
        elif editor_index["value"] is not None:
            editor_index["value"] -= sum(index < editor_index["value"] for index in indices)
        refresh_tree()

    def set_selected_group():
        indices = selected_indices()
        if not indices:
            messagebox.showerror("設定エラー", "グループ化する行を選択してください。")
            return

        base_index = indices[0]
        base_item = input_items[base_index]
        new_group = sanitize_filename(group_var.get() or base_item.get("group") or base_item.get("name"))
        new_field = field_var.get() or base_item.get("field")
        new_is_extra = bool(extra_var.get())
        new_mapping = base_item.get("value_mapping")
        new_reclass = base_item.get("value_reclass")

        for index in indices:
            if not input_items[index].get("output_group_user_set", False):
                input_items[index]["output_group"] = new_group
            input_items[index]["group"] = new_group
            input_items[index]["group_user_set"] = True
            input_items[index]["field"] = new_field
            input_items[index]["is_extra"] = new_is_extra
            if input_items[index].get("fixed_value") is not None:
                input_items[index]["field"] = ""
            if new_mapping:
                input_items[index]["value_mapping"] = dict(new_mapping)
                input_items[index].pop("value_reclass", None)
            if new_reclass:
                input_items[index]["value_reclass"] = dict(new_reclass)
                input_items[index].pop("value_mapping", None)
            update_item_conversion_warning(input_items[index])

        refresh_tree()
        tree.selection_set(*(str(index) for index in indices))
        editor_index["value"] = base_index
        load_selected_to_editor()

    def set_selected_output_group():
        indices = selected_indices()
        if not indices:
            messagebox.showerror("設定エラー", "河川出力グループを設定する行を選択してください。")
            return

        base_index = indices[0]
        base_item = input_items[base_index]
        new_output_group = sanitize_filename(
            output_group_var.get()
            or base_item.get("output_group")
            or base_item.get("group")
            or base_item.get("name")
        )

        for index in indices:
            input_items[index]["output_group"] = new_output_group
            input_items[index]["output_group_user_set"] = (
                new_output_group != input_items[index].get("group")
            )

        refresh_tree()
        tree.selection_set(*(str(index) for index in indices))
        editor_index["value"] = base_index
        load_selected_to_editor()

    def load_selected_to_editor(_event=None):
        index = selected_index()
        if index is None:
            editor_index["value"] = None
            field_combo.configure(values=())
            field_var.set("")
            name_var.set("")
            group_var.set("")
            output_group_var.set("")
            fixed_rank_var.set(False)
            fixed_value_var.set("")
            warning_var.set("")
            extra_var.set(False)
            return

        editor_index["value"] = index
        item = input_items[index]
        fields = item.get("fields")
        if fields is None:
            try:
                fields = get_attribute_fields(item["path"])
            except Exception:
                fields = []
            item["fields"] = fields

        field_combo.configure(values=fields)
        field_var.set(item.get("field") or choose_default_field(fields))
        fixed_value = item.get("fixed_value")
        fixed_rank_var.set(fixed_value is not None)
        fixed_value_var.set("" if fixed_value is None else str(fixed_value))
        name_var.set(item.get("name", ""))
        group_var.set(item.get("group") or item.get("name", ""))
        output_group_var.set(item.get("output_group") or item.get("group") or item.get("name", ""))
        warning_var.set(
            f"{item.get('warning')} {item.get('warning_message', '')}"
            if item.get("warning")
            else ""
        )
        extra_var.set(bool(item.get("is_extra")))

    def apply_editor_to_selected(_event=None):
        index = editor_index["value"]
        if index is None or index >= len(input_items):
            return

        old_group = input_items[index].get("group")
        old_output_group = input_items[index].get("output_group") or old_group
        old_output_group_user_set = bool(input_items[index].get("output_group_user_set", False))
        new_group = sanitize_filename(group_var.get())
        new_output_group = sanitize_filename(output_group_var.get() or new_group)
        if old_group != new_group and not old_output_group_user_set and old_output_group == old_group:
            new_output_group = new_group
            output_group_var.set(new_group)
        new_field = field_var.get()
        new_is_extra = bool(extra_var.get())
        new_mapping = input_items[index].get("value_mapping")
        new_reclass = input_items[index].get("value_reclass")
        fixed_value = None
        if fixed_rank_var.get():
            if not fixed_value_var.get().strip():
                messagebox.showerror("設定エラー", "固定rankを使用する場合はrank整数を入力してください。")
                return
            try:
                fixed_value = normalize_fixed_value(fixed_value_var.get())
            except InputDataError as e:
                messagebox.showerror("設定エラー", str(e))
                return

        input_items[index]["group"] = new_group
        if old_group != new_group:
            input_items[index]["group_user_set"] = True
        input_items[index]["output_group"] = new_output_group
        input_items[index]["output_group_user_set"] = new_output_group != new_group
        input_items[index]["field"] = "" if fixed_value is not None else new_field
        input_items[index]["fixed_value"] = fixed_value
        if fixed_value is not None:
            input_items[index].pop("value_mapping", None)
            input_items[index].pop("value_reclass", None)
        input_items[index]["name"] = sanitize_filename(name_var.get())
        input_items[index]["is_extra"] = new_is_extra
        update_item_conversion_warning(input_items[index])

        if new_group:
            for item in input_items:
                if item is not input_items[index] and item.get("group") == new_group:
                    item_fixed_value = item.get("fixed_value")
                    if item_fixed_value is not None:
                        item["field"] = ""
                        item.pop("value_mapping", None)
                        item.pop("value_reclass", None)
                    elif fixed_value is None:
                        item["field"] = new_field
                    item["is_extra"] = new_is_extra
                    item["output_group"] = new_output_group
                    item["output_group_user_set"] = new_output_group != new_group
                    if fixed_value is None and item_fixed_value is None and new_mapping:
                        item["value_mapping"] = dict(new_mapping)
                        item.pop("value_reclass", None)
                    if fixed_value is None and item_fixed_value is None and new_reclass:
                        item["value_reclass"] = dict(new_reclass)
                        item.pop("value_mapping", None)
                    update_item_conversion_warning(item)

        if old_group and old_group != new_group:
            for item in input_items:
                if item.get("group") == old_group and not item.get("field"):
                    item["field"] = new_field
                    update_item_conversion_warning(item)

        refresh_tree()
        if editor_index["value"] is not None and editor_index["value"] < len(input_items):
            item = input_items[editor_index["value"]]
            warning_var.set(
                f"{item.get('warning')} {item.get('warning_message', '')}"
                if item.get("warning")
                else ""
            )

    def on_fixed_rank_toggle():
        if fixed_rank_var.get():
            warning_var.set("固定rankを使用する場合はrank整数を入力してください。")
            if fixed_value_var.get().strip():
                apply_editor_to_selected()
            return
        fixed_value_var.set("")
        apply_editor_to_selected()

    def get_clip_bounds_from_gui():
        values = [var.get().strip() for var in clip_vars]
        if not any(values):
            return None
        if not all(values):
            raise InputDataError(
                '警告：矩形範囲を指定する場合は minx, miny, maxx, maxy をすべて入力してください。'
            )
        return [float(value) for value in values]

    def choose_output_file():
        path = filedialog.asksaveasfilename(
            title="出力シェープファイルを指定",
            initialfile=Path(output_path_var.get() or DEFAULT_OUTPUT_FILE).name,
            defaultextension=".shp",
            filetypes=(("Shapefile", "*.shp"), ("All files", "*.*")),
        )
        if path:
            output_path_var.set(path)

    def open_value_mapping_editor():
        index = selected_index()
        if index is None:
            messagebox.showerror("設定エラー", "変換表を設定する行を選択してください。")
            return

        item = input_items[index]
        if item.get("fixed_value") is not None:
            messagebox.showinfo("変換表", "固定rankを使用する行では変換表は不要です。")
            return
        if item.get("value_reclass"):
            messagebox.showinfo("変換表", "式テンプレートを使用する行では個別の変換表は不要です。")
            return

        field = item.get("field")
        if not field:
            messagebox.showerror("設定エラー", "読み取りフィールドを選択してください。")
            return

        group = item.get("group") or item.get("name") or Path(item["path"]).stem
        existing_mapping = normalize_value_mapping(item.get("value_mapping")) or {}
        try:
            unique_values, total_unique_count = get_attribute_unique_text_values(item["path"], field)
        except Exception as e:
            messagebox.showerror("変換表エラー", str(e))
            return

        values = sorted(set(unique_values) | set(existing_mapping))
        if not values:
            messagebox.showinfo("変換表", "変換表に設定する値がありません。")
            return

        dialog = tk.Toplevel(root)
        dialog.title("文字列からrankへの変換表")
        dialog.transient(root)
        dialog.resizable(True, True)
        dialog.grab_set()
        dialog.columnconfigure(0, weight=1)
        dialog.rowconfigure(1, weight=1)

        info_text = (
            f"グループ「{group}」のフィールド「{field}」に適用します。"
            "左の文字列に対応するrank整数を入力してください。"
        )
        if total_unique_count > len(unique_values):
            info_text += f" 表示は先頭{len(unique_values)}件です。未表示値がある場合は実行時にエラーになります。"
        ttk.Label(dialog, text=info_text, wraplength=760).grid(
            row=0,
            column=0,
            padx=10,
            pady=(10, 6),
            sticky="ew",
        )

        table_frame = ttk.Frame(dialog)
        table_frame.grid(row=1, column=0, padx=10, pady=4, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        canvas = tk.Canvas(table_frame, highlightthickness=0, width=780, height=360)
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=canvas.yview)
        rows_frame = ttk.Frame(canvas)
        rows_frame.columnconfigure(0, weight=1)
        rows_frame.columnconfigure(1, weight=0)

        canvas_window = canvas.create_window((0, 0), window=rows_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        def update_scroll_region(_event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))
            canvas.itemconfigure(canvas_window, width=canvas.winfo_width())

        rows_frame.bind("<Configure>", update_scroll_region)
        canvas.bind("<Configure>", update_scroll_region)

        ttk.Label(rows_frame, text="元の値").grid(row=0, column=0, padx=4, pady=4, sticky="w")
        ttk.Label(rows_frame, text="rank").grid(row=0, column=1, padx=4, pady=4, sticky="w")

        mapping_vars = {}
        for row_index, value in enumerate(values, 1):
            ttk.Label(rows_frame, text=value, wraplength=620).grid(
                row=row_index,
                column=0,
                padx=4,
                pady=2,
                sticky="ew",
            )
            rank_var = tk.StringVar(value=str(existing_mapping.get(value, "")))
            mapping_vars[value] = rank_var
            ttk.Entry(rows_frame, textvariable=rank_var, width=10).grid(
                row=row_index,
                column=1,
                padx=4,
                pady=2,
                sticky="w",
            )

        footer = ttk.Frame(dialog)
        footer.grid(row=2, column=0, padx=10, pady=(6, 10), sticky="ew")

        def save_mapping():
            new_mapping = {}
            missing_values = []
            invalid_values = []
            for source_value, rank_var in mapping_vars.items():
                raw_rank = rank_var.get().strip()
                if not raw_rank:
                    missing_values.append(source_value)
                    continue
                numeric_rank = pd.to_numeric(pd.Series([raw_rank]), errors="coerce").iloc[0]
                if pd.isna(numeric_rank) or numeric_rank % 1 != 0:
                    invalid_values.append(source_value)
                    continue
                new_mapping[source_value] = int(numeric_rank)

            if missing_values:
                messagebox.showerror(
                    "変換表エラー",
                    f"rankが未入力の値があります: {format_limited_values(missing_values)}",
                    parent=dialog,
                )
                return
            if invalid_values:
                messagebox.showerror(
                    "変換表エラー",
                    f"rankが整数ではない値があります: {format_limited_values(invalid_values)}",
                    parent=dialog,
                )
                return

            for target_item in input_items:
                if target_item.get("group") == group:
                    target_item["value_mapping"] = dict(new_mapping)
                    target_item.pop("value_reclass", None)
                    update_item_conversion_warning(target_item)

            refresh_tree()
            tree.selection_set(str(index))
            load_selected_to_editor()
            dialog.grab_release()
            dialog.destroy()

        ttk.Button(footer, text="保存", command=save_mapping).pack(side="right", padx=(6, 0))
        ttk.Button(
            footer,
            text="キャンセル",
            command=lambda: (dialog.grab_release(), dialog.destroy()),
        ).pack(side="right")

    def open_value_reclass_editor():
        index = selected_index()
        if index is None:
            messagebox.showerror("設定エラー", "式テンプレートを設定する行を選択してください。")
            return

        item = input_items[index]
        if item.get("fixed_value") is not None:
            messagebox.showinfo("式テンプレート", "固定rankを使用する行では式テンプレートは不要です。")
            return

        field = item.get("field")
        if not field:
            messagebox.showerror("設定エラー", "読み取りフィールドを選択してください。")
            return

        group = item.get("group") or item.get("name") or Path(item["path"]).stem
        existing_reclass = normalize_value_reclass(item.get("value_reclass"))
        selected_template_var = tk.StringVar()
        if existing_reclass:
            selected_template_var.set(VALUE_RECLASS_ID_TO_LABEL[existing_reclass["template"]])
        else:
            selected_template_var.set(VALUE_RECLASS_TEMPLATE_CHOICES[0][0])

        dialog = tk.Toplevel(root)
        dialog.title("式テンプレート設定")
        dialog.transient(root)
        dialog.resizable(True, True)
        dialog.grab_set()
        dialog.columnconfigure(1, weight=1)
        dialog.rowconfigure(3, weight=1)

        ttk.Label(dialog, text=f"グループ「{group}」のフィールド「{field}」に適用します。").grid(
            row=0,
            column=0,
            columnspan=2,
            padx=10,
            pady=(10, 6),
            sticky="w",
        )
        ttk.Label(dialog, text="テンプレート").grid(row=1, column=0, padx=10, pady=6, sticky="w")
        template_combo = ttk.Combobox(
            dialog,
            textvariable=selected_template_var,
            values=[label for label, _template_id in VALUE_RECLASS_TEMPLATE_CHOICES],
            state="readonly",
            width=52,
        )
        template_combo.grid(row=1, column=1, padx=10, pady=6, sticky="ew")

        description_var = tk.StringVar()

        def update_description(_event=None):
            template_id = VALUE_RECLASS_LABEL_TO_ID[selected_template_var.get()]
            description_var.set(VALUE_RECLASS_TEMPLATES[template_id]["description"])

        ttk.Label(dialog, textvariable=description_var, wraplength=620).grid(
            row=2,
            column=0,
            columnspan=2,
            padx=10,
            pady=(4, 10),
            sticky="ew",
        )

        preview_frame = ttk.LabelFrame(dialog, text="変換プレビュー")
        preview_frame.grid(row=3, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="nsew")
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.rowconfigure(1, weight=1)

        preview_message_var = tk.StringVar()
        ttk.Label(preview_frame, textvariable=preview_message_var, wraplength=620).grid(
            row=0,
            column=0,
            padx=6,
            pady=(6, 4),
            sticky="ew",
        )

        preview_tree = ttk.Treeview(
            preview_frame,
            columns=("rank", "count"),
            show="headings",
            height=8,
        )
        preview_tree.heading("rank", text="変換後rank")
        preview_tree.heading("count", text="件数")
        preview_tree.column("rank", width=160, stretch=True, anchor="center")
        preview_tree.column("count", width=160, stretch=True, anchor="e")
        preview_tree.grid(row=1, column=0, padx=(6, 0), pady=(0, 6), sticky="nsew")

        preview_scroll = ttk.Scrollbar(preview_frame, orient="vertical", command=preview_tree.yview)
        preview_scroll.grid(row=1, column=1, padx=(0, 6), pady=(0, 6), sticky="ns")
        preview_tree.configure(yscrollcommand=preview_scroll.set)

        preview_queue = queue.Queue()
        preview_state = {"running": False}

        def clear_preview_rows():
            for row_id in preview_tree.get_children():
                preview_tree.delete(row_id)

        def on_template_changed(_event=None):
            update_description()
            clear_preview_rows()
            preview_message_var.set("計算ボタンを押すと、同一グループ内の全レコードを集計します。")

        def calculate_file_counts(target_item, template_id):
            target_field = target_item.get("field") or field
            values = get_attribute_field_values(
                target_item["path"],
                target_field,
                max_features=None,
            )
            converted = apply_value_reclass(
                values,
                {"template": template_id},
                target_item["path"],
                target_field,
            )
            return {
                "path": target_item["path"],
                "record_count": int(len(values)),
                "rank_counts": converted.astype(int).value_counts().to_dict(),
            }

        def calculate_preview():
            if preview_state["running"]:
                return

            clear_preview_rows()
            template_id = VALUE_RECLASS_LABEL_TO_ID[selected_template_var.get()]

            target_items = [
                target_item
                for target_item in input_items
                if (
                    (target_item.get("group") or target_item.get("name") or Path(target_item["path"]).stem) == group
                    and target_item.get("fixed_value") is None
                    and Path(target_item.get("path", "")).exists()
                )
            ]
            if not target_items:
                preview_message_var.set("計算対象の入力ファイルがありません。")
                return

            preview_state["running"] = True
            calculate_button.configure(state="disabled")
            preview_message_var.set(
                f"計算中です。対象: {len(target_items)}ファイル。"
                "全レコードを読み込んでrank別件数を集計しています。"
            )

            def worker():
                from concurrent.futures import ThreadPoolExecutor, as_completed

                total_counts = {}
                total_records = 0
                errors = []
                max_workers = min(4, len(target_items))
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_item = {
                        executor.submit(calculate_file_counts, target_item, template_id): target_item
                        for target_item in target_items
                    }
                    for completed_count, future in enumerate(as_completed(future_to_item), 1):
                        try:
                            result = future.result()
                            total_records += result["record_count"]
                            for rank, count in result["rank_counts"].items():
                                total_counts[int(rank)] = total_counts.get(int(rank), 0) + int(count)
                        except Exception as e:
                            target_item = future_to_item[future]
                            errors.append(f'{target_item["path"]}: {e}')
                        preview_queue.put(("progress", completed_count, len(target_items)))

                preview_queue.put(("done", total_counts, total_records, errors))

            def poll_preview_queue():
                if not dialog.winfo_exists():
                    return

                while True:
                    try:
                        message = preview_queue.get_nowait()
                    except queue.Empty:
                        break

                    if message[0] == "progress":
                        _kind, completed_count, total_count = message
                        preview_message_var.set(
                            f"計算中です。{completed_count}/{total_count}ファイルを処理しました。"
                        )
                    elif message[0] == "done":
                        _kind, total_counts, total_records, errors = message
                        clear_preview_rows()
                        for row_index, rank in enumerate(sorted(total_counts), 1):
                            preview_tree.insert(
                                "",
                                "end",
                                iid=str(row_index),
                                values=(rank, total_counts[rank]),
                            )

                        if errors:
                            preview_message_var.set(
                                f"計算は完了しましたが、{len(errors)}ファイルでエラーがありました。"
                                f"総レコード数: {total_records}"
                            )
                            messagebox.showerror(
                                "プレビュー計算エラー",
                                "一部ファイルの計算に失敗しました。\n\n"
                                + "\n".join(errors[:10]),
                                parent=dialog,
                            )
                        else:
                            preview_message_var.set(
                                f"計算完了。対象: {len(target_items)}ファイル、総レコード数: {total_records}"
                            )

                        preview_state["running"] = False
                        calculate_button.configure(state="normal")

                if preview_state["running"]:
                    dialog.after(100, poll_preview_queue)

            threading.Thread(target=worker, daemon=True).start()
            dialog.after(100, poll_preview_queue)

        calculate_button = ttk.Button(
            preview_frame,
            text="計算",
            command=calculate_preview,
        )
        calculate_button.grid(row=0, column=1, padx=6, pady=(6, 4), sticky="e")

        template_combo.bind("<<ComboboxSelected>>", on_template_changed)
        on_template_changed()

        footer = ttk.Frame(dialog)
        footer.grid(row=4, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="ew")

        def save_reclass():
            template_id = VALUE_RECLASS_LABEL_TO_ID[selected_template_var.get()]
            new_reclass = {"template": template_id}
            for target_item in input_items:
                if target_item.get("group") == group:
                    target_item["value_reclass"] = dict(new_reclass)
                    target_item.pop("value_mapping", None)
                    update_item_conversion_warning(target_item)

            refresh_tree()
            tree.selection_set(str(index))
            load_selected_to_editor()
            dialog.grab_release()
            dialog.destroy()

        def clear_reclass():
            for target_item in input_items:
                if target_item.get("group") == group:
                    target_item.pop("value_reclass", None)
                    update_item_conversion_warning(target_item)

            refresh_tree()
            tree.selection_set(str(index))
            load_selected_to_editor()
            dialog.grab_release()
            dialog.destroy()

        ttk.Button(footer, text="解除", command=clear_reclass).pack(side="left")
        ttk.Button(footer, text="保存", command=save_reclass).pack(side="right", padx=(6, 0))
        ttk.Button(
            footer,
            text="キャンセル",
            command=lambda: (dialog.grab_release(), dialog.destroy()),
        ).pack(side="right")

    def get_output_epsg_from_gui():
        label = output_crs_var.get()
        if label in OUTPUT_CRS_LABEL_TO_EPSG:
            return OUTPUT_CRS_LABEL_TO_EPSG[label]

        match = re.search(r'EPSG:(\d+)', label)
        if match:
            return int(match.group(1))

        raise InputDataError('警告：出力座標系を選択してください。')

    def select_bounds_from_map():
        if not input_items:
            messagebox.showerror("設定エラー", "入力ファイルを1件以上追加してください。")
            return

        stop_loading = {"value": False}
        progress_dialog = tk.Toplevel(root)
        progress_dialog.title("地図用シェープ読込")
        progress_dialog.transient(root)
        progress_dialog.resizable(False, False)
        progress_dialog.grab_set()

        progress_label = ttk.Label(progress_dialog, text="地図表示用のシェープファイルを読み込んでいます。")
        progress_label.grid(row=0, column=0, padx=12, pady=(12, 4), sticky="w")
        progress_percent_label = ttk.Label(progress_dialog, text="0%")
        progress_percent_label.grid(row=0, column=1, padx=12, pady=(12, 4), sticky="e")

        progress_bar = ttk.Progressbar(progress_dialog, length=360, mode="determinate", maximum=100)
        progress_bar.grid(row=1, column=0, columnspan=2, padx=12, pady=4, sticky="ew")

        progress_file_label = ttk.Label(progress_dialog, text="", width=64)
        progress_file_label.grid(row=2, column=0, columnspan=2, padx=12, pady=4, sticky="w")

        def stop_and_show_basemap():
            stop_loading["value"] = True
            progress_label.configure(text="シェープ読込を中断します。背景地図のみ表示します。")

        ttk.Button(
            progress_dialog,
            text="シェープ読込を中断して背景地図のみ表示",
            command=stop_and_show_basemap,
        ).grid(row=3, column=0, columnspan=2, padx=12, pady=(6, 12), sticky="ew")
        progress_dialog.protocol("WM_DELETE_WINDOW", stop_and_show_basemap)

        def progress_callback(done, total, path, stopped):
            percent = int(done / total * 100) if total else 100
            progress_bar["value"] = percent
            progress_percent_label.configure(text=f"{percent}%")
            progress_file_label.configure(text=Path(path).name if path else "")
            if stopped:
                progress_label.configure(text="背景地図のみ表示します。")
            progress_dialog.update()

        try:
            current_bounds = get_clip_bounds_from_gui()
            selected_bounds = select_clip_bounds_on_map(
                input_items,
                current_bounds,
                progress_callback=progress_callback,
                stop_loading_callback=lambda: stop_loading["value"],
            )
        except Exception as e:
            messagebox.showerror("地図選択エラー", str(e))
            return
        finally:
            if progress_dialog.winfo_exists():
                progress_dialog.grab_release()
                progress_dialog.destroy()

        if selected_bounds is None:
            return

        for index, value in enumerate(selected_bounds):
            clip_vars[index].set(str(value))

    def make_gui_config():
        if editor_index["value"] is not None and fixed_rank_var.get():
            if not fixed_value_var.get().strip():
                raise InputDataError('警告：固定rankを使用する場合はrank整数を入力してください。')
            normalize_fixed_value(fixed_value_var.get())
        apply_editor_to_selected()
        if not input_items:
            raise InputDataError('警告：入力ファイルを1件以上追加してください。')
        if not output_path_var.get().strip():
            raise InputDataError('警告：出力ファイルを指定してください。')
        if not output_field_var.get().strip():
            raise InputDataError('警告：出力フィールド名を指定してください。')

        inputs = []
        for item in input_items:
            config_item = {
                "path": item["path"],
                "field": item.get("field"),
                "is_extra": bool(item.get("is_extra")),
                "name": item.get("name") or Path(item["path"]).stem,
                "group": item.get("group") or item.get("name") or Path(item["path"]).stem,
                "group_user_set": bool(item.get("group_user_set", False)),
                "output_group": item.get("output_group") or item.get("group") or item.get("name") or Path(item["path"]).stem,
                "output_group_user_set": bool(item.get("output_group_user_set", False)),
            }
            if item.get("fixed_value") is not None:
                config_item["fixed_value"] = item.get("fixed_value")
            if item.get("value_mapping"):
                config_item["value_mapping"] = dict(item["value_mapping"])
            if item.get("value_reclass"):
                config_item["value_reclass"] = dict(item["value_reclass"])
            inputs.append(config_item)

        return {
            "version": 1,
            "processing": {
                "dissolve_input_by_value": bool(dissolve_var.get()),
                "clip_bounds": get_clip_bounds_from_gui(),
                "output_path": output_path_var.get().strip(),
                "output_field": output_field_var.get().strip(),
                "output_epsg": get_output_epsg_from_gui(),
                "output_group_files": bool(output_group_files_var.get()),
                "keep_intermediate_files": bool(keep_intermediate_var.get()),
            },
            "inputs": inputs,
        }

    def save_config_from_gui():
        try:
            config = normalize_config(make_gui_config(), require_existing_paths=False)
        except Exception as e:
            messagebox.showerror("設定エラー", str(e))
            return

        path = filedialog.asksaveasfilename(
            title="YAML設定を保存",
            defaultextension=".yaml",
            filetypes=(("YAML", "*.yaml *.yml"), ("All files", "*.*")),
        )
        if not path:
            return

        try:
            save_yaml_config(config, path)
        except Exception as e:
            messagebox.showerror("保存エラー", str(e))

    def load_config_to_gui():
        path = filedialog.askopenfilename(
            title="YAML設定を読み込み",
            filetypes=(("YAML", "*.yaml *.yml"), ("All files", "*.*")),
        )
        if not path:
            return

        try:
            config = normalize_config(load_yaml_config(path), require_existing_paths=False)
        except Exception as e:
            messagebox.showerror("読込エラー", str(e))
            return

        input_items.clear()
        missing_count = 0
        for item in config["inputs"]:
            fields = []
            try:
                if Path(item["path"]).exists():
                    fields = get_attribute_fields(item["path"])
            except Exception:
                pass
            item = dict(item)
            item["fields"] = fields
            item["group"] = item.get("group") or item.get("name") or Path(item["path"]).stem
            if "group_user_set" not in item:
                item["group_user_set"] = sanitize_filename(item["group"]) != sanitize_filename(Path(item["path"]).stem)
            else:
                item["group_user_set"] = bool(item.get("group_user_set"))
            item["output_group"] = item.get("output_group") or item.get("group")
            item["output_group_user_set"] = bool(item.get("output_group_user_set", False))
            update_item_conversion_warning(item)
            if item.get("missing_path"):
                missing_count += 1
            input_items.append(item)

        processing = config["processing"]
        dissolve_var.set(bool(processing.get("dissolve_input_by_value", False)))
        keep_intermediate_var.set(bool(processing.get("keep_intermediate_files", False)))
        bounds = processing.get("clip_bounds") or ["", "", "", ""]
        for index, value in enumerate(bounds):
            clip_vars[index].set("" if value is None else str(value))
        output_path_var.set(processing.get("output_path") or str(DEFAULT_OUTPUT_FILE))
        output_field_var.set(processing.get("output_field") or DEFAULT_OUTPUT_FIELD)
        output_epsg = int(processing.get("output_epsg") or DEFAULT_OUTPUT_EPSG)
        output_crs_var.set(
            OUTPUT_CRS_EPSG_TO_LABEL.get(output_epsg, f"EPSG:{output_epsg}")
        )
        output_group_files_var.set(bool(processing.get("output_group_files", False)))

        refresh_tree()
        load_selected_to_editor()
        if missing_count:
            messagebox.showwarning(
                "読込警告",
                f"{missing_count}件の入力ファイルが存在しません。一覧の▲行を確認してください。",
            )

    def load_spatial_settings_from_yaml():
        path = filedialog.askopenfilename(
            title="YAMLから処理範囲と出力座標系を読み込み",
            filetypes=(("YAML", "*.yaml *.yml"), ("All files", "*.*")),
        )
        if not path:
            return

        try:
            config = load_yaml_config(path)
            processing = dict(config.get("processing") or {})
            loaded = []

            if "clip_bounds" in processing:
                bounds = processing.get("clip_bounds")
                if bounds in (None, ""):
                    bounds = ["", "", "", ""]
                if len(bounds) != 4:
                    raise InputDataError(
                        "警告：clip_bounds は [minx, miny, maxx, maxy] の4値で指定してください。"
                    )
                for index, value in enumerate(bounds):
                    clip_vars[index].set("" if value is None else str(value))
                loaded.append("処理範囲")

            if "output_epsg" in processing:
                output_epsg = int(processing.get("output_epsg") or DEFAULT_OUTPUT_EPSG)
                output_crs_var.set(
                    OUTPUT_CRS_EPSG_TO_LABEL.get(output_epsg, f"EPSG:{output_epsg}")
                )
                loaded.append("出力座標系")
        except Exception as e:
            messagebox.showerror("読込エラー", str(e))
            return

        if not loaded:
            messagebox.showwarning(
                "読込警告",
                "選択したYAMLに処理範囲または出力座標系の設定が見つかりませんでした。",
            )
            return

        messagebox.showinfo(
            "読込完了",
            "、".join(loaded) + "を読み込みました。",
        )

    def set_button_frame_state(state: str):
        for child in button_frame.winfo_children():
            if "state" in child.keys():
                child.configure(state=state)

    def run_from_gui():
        try:
            config = normalize_config(make_gui_config())
        except Exception as e:
            messagebox.showerror("設定エラー", str(e))
            return

        log_text.configure(state="normal")
        log_text.delete("1.0", "end")
        log_text.configure(state="disabled")

        set_button_frame_state("disabled")

        def worker():
            try:
                with contextlib.redirect_stdout(QueueWriter(output_queue)):
                    run_pipeline(config)
            except Exception as e:
                output_queue.put(("error", str(e)))
            else:
                output_queue.put(("done", "処理が完了しました。"))

        worker_thread["thread"] = threading.Thread(target=worker, daemon=False)
        worker_thread["thread"].start()

    def poll_output_queue():
        try:
            while True:
                message_type, payload = output_queue.get_nowait()
                if message_type == "log":
                    log(payload)
                elif message_type == "error":
                    set_button_frame_state("normal")
                    worker_thread["thread"] = None
                    messagebox.showerror("処理エラー", payload)
                elif message_type == "done":
                    set_button_frame_state("normal")
                    worker_thread["thread"] = None
                    messagebox.showinfo("完了", payload)
        except queue.Empty:
            pass
        root.after(100, poll_output_queue)

    ttk.Button(button_frame, text="追加", command=add_files).pack(side="left", padx=(0, 6))
    ttk.Button(button_frame, text="削除", command=remove_selected).pack(side="left", padx=(0, 6))
    ttk.Separator(button_frame, orient="vertical").pack(side="left", fill="y", padx=(2, 8))
    ttk.Button(button_frame, text="選択を同一河川出力グループ", command=set_selected_output_group).pack(side="left", padx=(0, 6))
    ttk.Button(button_frame, text="選択を同一データグループ", command=set_selected_group).pack(side="left", padx=(0, 6))
    ttk.Separator(button_frame, orient="vertical").pack(side="left", fill="y", padx=(2, 8))
    ttk.Button(button_frame, text="変換表設定", command=open_value_mapping_editor).pack(side="left", padx=(0, 6))
    ttk.Button(button_frame, text="式テンプレート", command=open_value_reclass_editor).pack(side="left", padx=(0, 6))
    ttk.Separator(button_frame, orient="vertical").pack(side="left", fill="y", padx=(2, 8))
    ttk.Button(button_frame, text="YAML読込", command=load_config_to_gui).pack(side="left", padx=(0, 6))
    ttk.Button(button_frame, text="YAML保存", command=save_config_from_gui).pack(side="left", padx=(0, 6))
    ttk.Button(button_frame, text="実行", command=run_from_gui).pack(side="right")
    bounds_button_frame = ttk.Frame(bounds_frame)
    bounds_button_frame.grid(
        row=2,
        column=0,
        columnspan=10,
        padx=6,
        pady=(0, 6),
        sticky="w",
    )
    ttk.Button(bounds_button_frame, text="地図で範囲選択", command=select_bounds_from_map).pack(
        side="left",
        padx=(0, 6),
    )
    ttk.Button(
        bounds_button_frame,
        text="YAMLから範囲/座標系読込",
        command=load_spatial_settings_from_yaml,
    ).pack(
        side="left",
    )

    tree.bind("<<TreeviewSelect>>", load_selected_to_editor)
    field_combo.bind("<<ComboboxSelected>>", apply_editor_to_selected)
    name_entry.bind("<FocusOut>", apply_editor_to_selected)
    group_entry.bind("<FocusOut>", apply_editor_to_selected)
    output_group_entry.bind("<FocusOut>", apply_editor_to_selected)
    fixed_rank_check.configure(command=on_fixed_rank_toggle)
    fixed_value_entry.bind("<FocusOut>", apply_editor_to_selected)
    extra_check.configure(command=apply_editor_to_selected)

    def on_close():
        thread = worker_thread.get("thread")
        if thread is not None and thread.is_alive():
            messagebox.showwarning("処理中", "処理実行中です。完了してから閉じてください。")
            return
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    poll_output_queue()
    root.mainloop()
