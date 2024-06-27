import argparse
import json
import random
import re
import time
from typing import Any

from faker import Faker
from models import (
    ColumnChangeSettings,
    ForeignKeyData,
    ForeignKeyReference,
    TableChangeSettings,
    TableColumn,
    TableData,
)
from rstr import xeger

RESERVED_KEYWORDS = ["rows", "order", "columns", "description"]


def line_to_list(line: str) -> list[str]:
    clean_line = line.strip("() ;").replace("`", "")
    ret = re.findall("'[^']*'|[^,]+(?=,)|(?<=\")[^,]+(?=,)|(?<=,)[^,]+", clean_line)
    return ret


def parse_table_structure(table_name: str, file) -> TableData:
    table_columns = []
    foreign_keys = []
    for line in file:
        line = line.decode("utf-8").lower().strip()
        accepted_line_starts = ["`", "primary key", "unique key", "key", "constraint"]
        if not any(line.startswith(start) for start in accepted_line_starts):
            break
        if line.startswith("`"):
            split_line = line.split()
            column_name = split_line[0].strip("`").strip(",").lower()
            column_type = split_line[1].strip(",")
            table_columns.append(TableColumn(name=column_name, sql_data_type=column_type))
        elif line.startswith("constraint"):
            # Line structure:
            # CONSTRAINT `key_name` FOREIGN KEY (`column_name`) REFERENCES `other_table` (`other_table_column_name`)
            # Example:
            # CONSTRAINT `fk-test-tracker_code-tracker` FOREIGN KEY (`tracker_code`) REFERENCES `tracker` (`code`)
            _, column_name, other_table, other_table_column_name = re.findall(
                r"constraint `([^`]+)` foreign key \(`([^`]+)`\) references `([^`]+)` \(`([^`]+)`\)",
                line,
            )[0]
            foreign_keys.append(
                ForeignKeyData(
                    column_name=column_name,
                    referenced_table_name=other_table,
                    referenced_column_name=other_table_column_name,
                )
            )

    return TableData(table_name=table_name, table_columns=table_columns, foreign_keys=foreign_keys)


def read_dump_table_structure(dump_filename: str) -> list[TableData]:
    tables_data = []
    with open(dump_filename, "rb") as f:
        for i, line in enumerate(f):
            line = line.decode("utf-8").lower().strip()
            if line.startswith(f"create table"):
                table_name = line.split()[2].strip("`")
                tables_data.append(parse_table_structure(table_name, f))

    return tables_data


def read_dump_inserts(dump_filename: str, dump_structure: list[TableData]) -> dict[str, str]:
    tables_metadata = {
        table_data.table_name: [column.name for column in table_data.table_columns] for table_data in dump_structure
    }

    inserts_data: dict[str, str] = {}
    with open(dump_filename, "rb") as f:
        for i, line in enumerate(f):
            line = line.decode("utf-8").strip()
            if not line.startswith("INSERT INTO"):
                continue

            table_name = line.lower().split("`")[1]
            column_names = re.findall(r"\([^\)]*\)(?= VALUES)", line)
            if column_names:
                column_names = [name.strip(" ") for name in column_names[0].strip("() ").replace("`", "").split(",")]
            else:
                column_names = tables_metadata[table_name]

            insert_rows = [line_to_list(x) for x in re.findall("(?<=VALUES) .*", line)[0].split("),(")]
            joined_insert_rows = [",".join(row) for row in insert_rows]
            column_names = [f"`{column_name}`" for column_name in column_names]
            data = f"INSERT INTO `{table_name}` ({','.join(column_names)}) VALUES ({'),('.join(joined_insert_rows)});"

            if inserts_data.get(table_name):
                data = re.sub(rf"INSERT INTO `{table_name}` \([^\(\)]+\) VALUES ", ",", data)
                inserts_data[table_name] = inserts_data[table_name].rstrip(";\n") + data
            else:
                inserts_data[table_name] = data

    return inserts_data


def _get_fks(
    table_name: str, columns_to_change: list[str], structure: list[TableData]
) -> dict[str, list[ForeignKeyReference]]:
    columns_fk_referenced: dict[str, list[ForeignKeyReference]] = {}
    for table_data in structure:
        for fk in table_data.foreign_keys:
            if fk.referenced_table_name == table_name and fk.referenced_column_name in columns_to_change:
                fk_reference = ForeignKeyReference(table_name=table_data.table_name, column_name=fk.column_name)
                if columns_fk_referenced.get(fk.referenced_column_name):
                    columns_fk_referenced[fk.referenced_column_name].append(fk_reference)
                else:
                    columns_fk_referenced[fk.referenced_column_name] = [fk_reference]

    return columns_fk_referenced


def get_insert_column_names(insert_line: str) -> list[str]:
    columns_str = re.findall(r"\([^\)]*\)(?= VALUES)", insert_line)
    names = [name.strip(" ") for name in columns_str[0].strip("() ").replace("`", "").split(",")]

    for reserved_keyword in RESERVED_KEYWORDS:
        try:
            names[names.index(reserved_keyword)] = f"`{reserved_keyword}`"
        except ValueError:
            continue
    return names


def anonymize(
    inserts_dict: dict[str, str],
    structure: list[TableData],
    table_columns_to_change: list[TableChangeSettings],
) -> dict[str, str]:
    for table_settings in table_columns_to_change:
        table_name = table_settings.table_name
        columns_to_change = table_settings.columns_to_change
        table_structure = [table for table in structure if table.table_name == table_name][0]
        column_types = {column.name: column.sql_data_type for column in table_structure.table_columns}

        columns_to_change_names = [column.name for column in columns_to_change]
        columns_fk_referenced = _get_fks(table_name, columns_to_change_names, structure)
        try:
            insert_line = inserts_dict[table_name]
        except KeyError:
            continue
        column_names = get_insert_column_names(insert_line)

        column_names_and_indexes_to_change = [
            (
                column,
                (
                    column_names.index(column.name)
                    if column.name not in RESERVED_KEYWORDS
                    else column_names.index(f"`{column.name}`")
                ),
                column_types.get(column.name),
            )
            for column in columns_to_change
        ]
        new_line, changes = get_line_with_randomized_values(
            insert_line,
            table_name,
            column_names_and_indexes_to_change,
            column_names,
            columns_fk_referenced,
        )

        inserts_dict[table_name] = new_line
        inserts_dict = propagate_changes_in_fks(inserts_dict, columns_fk_referenced, changes)
    return inserts_dict


def get_line_with_randomized_values(
    line: str,
    table_name: str,
    column_names_and_indexes_to_change: list[tuple[ColumnChangeSettings, int, str]],
    columns_in_insert_statement: list[str],
    columns_fk_referenced: dict[str, list[ForeignKeyReference]],
) -> tuple[str, dict[str, dict[Any, Any]]]:
    insert_rows = [line_to_list(x) for x in re.findall("(?<=VALUES) .*", line)[0].split("),(")]
    faker = Faker()
    changes: dict[str, dict[Any, Any]] = {}

    for column, index, column_sql_type in column_names_and_indexes_to_change:
        column_name = column.name
        column_changes: dict[Any, Any] = {}
        for i, row in enumerate(insert_rows):
            old_value = row[index]
            if column.subtype == "UUID":
                row[index] = f"'{faker.uuid4()}'"
            elif column.subtype == "uri":
                row[index] = f"'{faker.uri()}'"
            elif column.regex is not None:
                row[index] = f"'{xeger(column.regex)}'"
            elif column_sql_type.startswith("datetime"):
                date = faker.date_time().strftime("%Y-%m-%d %H:%M:%S%L")
                row[index] = f"'{date}'"
            elif column_sql_type == "date":
                date = faker.date_time().strftime("%Y-%m-%d")
                row[index] = f"'{date}'"
            elif column_sql_type == "float":
                row[index] = (
                    f"{random.uniform(*column.interval):.3f}" if column.interval else f"{random.uniform(0, 1):.3f}"
                )
            elif column_sql_type == "int":
                row[index] = (
                    str(random.randint(*[int(endpoint) for endpoint in column.interval]))
                    if column.interval
                    else str(random.randint(0, 100))
                )
            elif column_sql_type.startswith("tinyint"):
                row[index] = random.choice([0, 1])
            elif column_sql_type.startswith("enum"):
                enum_values = column_sql_type[column_sql_type.index("m") + 1 :].strip("()").split(",")
                row[index] = random.choice(enum_values)
            else:
                row[index] = faker.lexify(f"'????-????-{i + 1}'")
            if column_name in columns_fk_referenced and not column_changes.get(old_value):
                column_changes[old_value] = row[index]
            elif column_name in columns_fk_referenced:
                row[index] = column_changes[old_value]
        if column_name in columns_fk_referenced:
            changes[column_name] = column_changes

    joined_insert_rows = [",".join(row) for row in insert_rows]
    return (
        f"INSERT INTO `{table_name}` ({','.join(columns_in_insert_statement)}) VALUES ({'),('.join(joined_insert_rows)});",
        changes,
    )


def propagate_changes_in_fks(
    inserts_dict: dict[str, str],
    columns_fk_referenced: dict[str, list[ForeignKeyReference]],
    changes: dict[str, dict[Any, Any]],
) -> dict[str, str]:
    for column_name, tables_reference in columns_fk_referenced.items():
        for table_reference in tables_reference:
            if not inserts_dict.get(table_reference.table_name):
                continue
            line = inserts_dict[table_reference.table_name]
            column_names = get_insert_column_names(line)
            column_index = column_names.index(table_reference.column_name)
            insert_rows = [line_to_list(x) for x in re.findall("(?<=VALUES) .*", line)[0].split("),(")]
            for row in insert_rows:
                row[column_index] = changes[column_name].get(row[column_index], "NULL")

            joined_insert_rows = [",".join(row) for row in insert_rows]
            new_line = (
                f"INSERT INTO `{table_reference.table_name}` "
                f"({','.join(column_names)}) VALUES ({'),('.join(joined_insert_rows)});"
            )
            inserts_dict[table_reference.table_name] = new_line

    return inserts_dict


def write_in_file(dump_filename: str, output_filename: str, lines: dict[str, str]) -> None:
    output_lines = []
    seen_tables = set()

    with open(dump_filename, "rb") as f:
        for i, line in enumerate(f):
            line = line.decode("utf-8").strip()
            if not line.startswith("INSERT INTO"):
                output_lines.append(line + "\n")
            else:
                table_name = line.lower().split("`")[1]
                if table_name in seen_tables:
                    continue
                output_lines.append(lines[table_name] + "\n")
                seen_tables.add(table_name)

    with open(output_filename, "w") as out:
        out.writelines(output_lines)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mysql_dump_anonymizer",
        description="CLI tool to anonymize a mysql database dump",
    )
    parser.add_argument("original_dump", help="File containing the original dump")
    parser.add_argument(
        "-t",
        "--target_file",
        default="anon_dump.sql",
        help="File to which the anonymized dump will be written",
    )
    parser.add_argument(
        "-c",
        "--config_file",
        help="JSON file containing settings pertaining to target tables and columns",
    )
    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()
    original = args.original_dump
    target = args.target_file
    config_file = args.config_file

    print(f"Preparing file {original}")
    with open(original):
        print(f"File {original} successfully opened")

    if config_file:
        with open(config_file) as settings_file:
            settings = json.load(settings_file)
        print(f"Config file '{config_file}' successfully loaded")
    else:
        raise Exception("No config file provided")

    parsed_settings = [TableChangeSettings.model_validate(table) for table in settings["tables"]]

    begin = time.time()

    tables_structure = read_dump_table_structure(original)
    inserts = read_dump_inserts(original, tables_structure)
    inserts = anonymize(inserts, tables_structure, parsed_settings)

    print(f"Writing changes in file {target}")
    write_in_file(original, target, inserts)
    print(f"Anonymized dump successfully written to {target} in {time.time() - begin:.2f} seconds")


if __name__ == "__main__":
    main()
