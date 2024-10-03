# import yaml
import pandas as pd
import sys

import psycopg2
from sqlalchemy import create_engine
from sql_script_generator import SQLScriptGenerator
import argparse
import os
from settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, OUTPUT_DIR, DB_SCHEMA, VIEW_GRANT_USER


SELECT_SENTENCE = " SELECT "
FROM_SENTENCE = "FROM "
MANDATORY_COLUMNS_FOR_COLLECTIONS = """project."projectId" AS "project id",
    cs."itemId" AS "item id" """
MANDATORY_COLUMNS_FOR_FORM = 'project."projectId" AS "project id"'
PROJECT_ID_NAME = {
    "877": "CAMP_LEJEUNE",
    "1398": "TRAIN",
    "1742": "VINEBOTS",
    "1946": "SINGLE_EVENT",
    "2103": "MT_LEADERSHIP",
    "2201": "HAIR_RELAXER",
    "2204": "AFFF",
    "2255": "PARTNER_FIRMS",
    "2374": "PARAQUAT",
    "2376": "EXPERTS",
}


def create_query_items(
    fields_map,
    contacts_fields,
    left_join_on="personId",
    original_table_name="",
    contacts_legacy_table_name="",
    additional_columns_to_show="",
):

    temporal_tables_sentences = []
    columns_definition_sentences = []
    join_sentences = []

    for row in fields_map:
        query_object = SQLScriptGenerator(
            field_selector=row["field_selector"],
            field_selector_left_join_on=left_join_on,
            field_name=row["field_name"],
            field_type=row["customFieldType"],
            original_table_name=original_table_name,
            contact_fields=contacts_fields[contacts_fields["field_selector"] == row["field_selector"]],
            contacts_legacy_table_name=contacts_legacy_table_name,
        )

        query_values = query_object.return_values()

        if query_values.get("temporal_tables_sentences"):
            if len(temporal_tables_sentences) == 0:
                temporal_tables_sentences.append("WITH " + query_values.get("temporal_tables_sentences"))
            else:
                temporal_tables_sentences.append(query_values.get("temporal_tables_sentences"))

        if query_values.get("columns_definition_sentences"):
            if len(columns_definition_sentences) == 0:
                if row["sectionType"] == "collections":
                    columns_definition_sentences.append(MANDATORY_COLUMNS_FOR_COLLECTIONS)
                elif row["sectionType"] == "form":
                    columns_definition_sentences.append(MANDATORY_COLUMNS_FOR_FORM)
            columns_definition_sentences.append(query_values.get("columns_definition_sentences"))
        if query_values.get("join_sentences"):
            join_sentences.append(query_values.get("join_sentences"))

    join_sentences.append(
        f"""LEFT JOIN {original_table_name} AS cs
            ON project."projectId" = cs."projectId" """
    )

    if additional_columns_to_show:
        columns_definition_sentences.append(",\n\t".join(additional_columns_to_show))

    temporal_tables_sentences = """, 
    """.join(
        temporal_tables_sentences
    )

    columns_definition_sentences = """ ,
    """.join(
        columns_definition_sentences
    )

    join_sentences = """ 
    """.join(
        join_sentences
    )

    query_result = f"""
    {temporal_tables_sentences}
    SELECT 
    {columns_definition_sentences}
    FROM current."fvdw_Project" AS project
    {join_sentences}"""

    if view["case_type_id"] != "base":
        query_result += f"""WHERE project."projectTypeId" = '{view["case_type_id"]}';"""
    else :
        query_result += ";"

    return query_result


def generate_query(
    view,
    ordered_map_dataframe,
    contact_cards_fields,
    additional_columns_to_show,
    owner,
):
    output_file = (
        f'../SQL/{PROJECT_ID_NAME.get(view["case_type_id"], "BASE")}/{view["view_name"]}_view_query_output.sql'
    )

    # Convert DataFrames to dictionaries
    ordered_map_records = ordered_map_dataframe.to_dict(orient="records")

    query_result = f"""DROP VIEW IF EXISTS current.{view["view_name"]};
    CREATE VIEW current.{view["view_name"]} AS
    """

    if view["case_type_id"] in ["base", "client", "seclient"]:
        original_table_name = """ current."fvdw_Project" """
    else:
        original_table_name = f'current."fvdw_{view["section_type"]}_{view["case_type_id"]}_{view["section_selector"]}"'

    query_result += create_query_items(
        fields_map=ordered_map_records,
        contacts_fields=contact_cards_fields,
        additional_columns_to_show=additional_columns_to_show,
        original_table_name=original_table_name,
        contacts_legacy_table_name='current."fvdw_Contact_legacy"',
    )

    query_result += f"""
    ALTER TABLE current.{view["view_name"]} OWNER TO "{owner}";
    COMMENT ON VIEW current.{view["view_name"]} IS 'in development view for current.{view["view_name"]}';
    GRANT ALL ON current.{view["view_name"]} TO GROUP "neo-bi-support-group" ;"""

    with open(output_file, "w") as f:
        f.write(query_result)

    # replaced_object = replace_variables(variables, objects)

    # with open(output_file, "w") as f:
    #    f.write(replaced_object)

    print(f"Output saved to {output_file}")


def get_fields_from_local_csv_source(view):
    fields_csv_template_location = f'../TEMPLATE_FIELDS/template_fields-{view["case_type_id"]}.csv'
    ordered_section_map_dataframe = pd.read_csv(fields_csv_template_location)

    if view["case_type_id"] not in ["base", "client", "seclient", "hrclient"]:
        ordered_section_map_dataframe = ordered_section_map_dataframe[
                ordered_section_map_dataframe["section_selector"] == view["section_selector"]
            ]
        
    return ordered_section_map_dataframe

def connect_to_db():
    return create_engine(
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
        )
    )

if __name__ == "__main__":
    # Obtener argumentos de la línea de comandos
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--views_list",
        default="../TEMPLATES/views_list.csv",
        help="Specify the views_list file location",
    )
    parser.add_argument(
        "--contacts_fields",
        default="../TEMPLATES/contacts_fields.csv",
        help="Specify the contacts_fields file location",
    )
    parser.add_argument(
        "--view_name",
        default=False,
        help="Specify a view name to only generate that one",
    )
    parser.add_argument(
        "--owner",
        default="neo-alejandrobarrientos",
        help="Specify the owner for the view",
    )
    parser.add_argument(
        "--source",
        default="aurora",
        help="Specify the source [aurora, csv_templates]",
    )

    args = parser.parse_args()

    views_list = pd.read_csv(args.views_list)

    if args.view_name:
        views_list = views_list[views_list["view_name"] == args.view_name]

    contacts_fields = pd.read_csv(args.contacts_fields)

    # File with contact card custom fields from FV
    contacts_csv_template_location = f"../TEMPLATE_FIELDS/get_custom_contact_fields.csv"

    # Read csv file with custom contacts fields
    contacts_df = pd.read_csv(contacts_csv_template_location).fillna("")

    # Add field name
    contacts_map_dataframe = pd.merge(
        contacts_fields,
        contacts_df,
        how="left",
        left_on="related_field_selector",
        right_on="contactFieldSelector",
    )

    for index, view in views_list.iterrows():
        # Select the contacts fields to be added for each field in a given section
        view_contacts = contacts_map_dataframe[contacts_fields["view_name"] == view["view_name"]]

        if args.source == 'aurora':
            conn = connect_to_db()
            query = f"""SELECT
                            PTF.*,
                            CASE
                                WHEN PTS."isCollection" THEN 'collections'
                                ELSE 'form'
                            END "sectionType"
                        FROM
                            CURRENT."fvdw_ProjectType_Sections" PTS
                            join CURRENT."fvdw_ProjectType_Fields" PTF ON PTF."sectionSelector" = PTS."sectionSelector"
                            AND PTF."projectTypeId" = {view["case_type_id"]}
                            AND PTF."sectionSelector" = '{view["section_selector"]}'
                            AND PTS."projectTypeId" = {view["case_type_id"]}"""
            print("#"*50 + " QUERY " + "#"*50)
            print(f"{query}")
            section_fields = pd.read_sql(query,conn)
            ordered_section_map_dataframe = section_fields.rename({
                "fieldSelector" : "field_selector",
                "name" : "field_name",
                "projectTypeId" : "customFieldType"
                
            }, axis=1,inplace=False)
        else :
            ordered_section_map_dataframe = get_fields_from_local_csv_source(view)

        # Additional columns (calculated columns)
        additional_columns_to_show = {}
        if os.path.isfile(
            f'../SQL/{PROJECT_ID_NAME.get(view["case_type_id"], "BASE")}/CALCULATED_COLUMNS/{view["view_name"]}.sql'
        ):
            with open(
                f'../SQL/{PROJECT_ID_NAME.get(view["case_type_id"], "BASE")}/CALCULATED_COLUMNS/{view["view_name"]}.sql',
                "r",
            ) as f:
                additional_columns_to_show = [line.strip() for line in f]

        generate_query(
            view=view,
            ordered_map_dataframe=ordered_section_map_dataframe,
            contact_cards_fields=view_contacts,
            additional_columns_to_show=additional_columns_to_show,
            owner=args.owner,
        )
