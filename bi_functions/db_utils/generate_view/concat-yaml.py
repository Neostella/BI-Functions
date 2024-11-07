import yaml
import pandas as pd
import sys
from settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, OUTPUT_DIR, DB_SCHEMA, VIEW_GRANT_USER


def read_template(template_file):
    """
    Read the YAML template and extract variables and objects.
    """
    with open(template_file, "r") as f:
        template_data = yaml.safe_load(f)

    variables = template_data.get("Variables", {})
    objects = template_data.get("Objects", [])
    query_variables = template_data.get("QueryVariables", {})

    return variables, objects, query_variables


def replace_variables(variables, objects):
    """
    Replace variables in objects.
    """

    objects = "\n".join(objects.values())

    for var_name, value in variables.items():

        if isinstance(value.get("type"), str) and value.get("type").startswith("file"):
            # If the value is a file, read its content
            filename = value.get("default")
            with open(filename, "r") as f:
                file_content = f.read()
            objects = objects.replace(f"{{{{{var_name}}}}}", file_content + "\n")
        else:
            objects = objects.replace(f"{{{{{var_name}}}}}", str(value.get("default")))
    return objects

def connect_to_db():
    from sqlalchemy import create_engine
    return create_engine(
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
        )
    )


def read_schema(variables, section_name,csv_template_location, order_file_txt = None):
   
    # if "/" in section_name:
    #     section_name_file_name = "_".join(section_name.lower().split("/"))
    # else:
    #     section_name_file_name = "_".join(section_name.lower().split(" "))

    if csv_template_location != 'aurora':
        df = pd.read_csv(csv_template_location)
        filtered_template = df[
            (df["section_name"] == section_name) | (df["section_selector"] == section_name)
    ]
    else :
        import re
        filtered_template = read_from_aurora(variables, re)

    if order_file_txt != None:

        map_template_df = generate_sorted_df(order_file_txt, filtered_template)
        return map_template_df

    else :
        return filtered_template

def read_from_aurora(variables, re):
    pattern = r'current\."fvdw_(Form|CollectionItem)_(\d+)_(\w+)"'
    match = re.search(pattern, variables["original_table_name"]["default"])

    if match:
        project_type_id = match.group(2)
        section_name = match.group(3)
        print(f'section_name = {section_name}')
        print(f'project_type_id = {project_type_id}')

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
                        AND PTF."projectTypeId" = {project_type_id}
                        AND PTF."sectionSelector" = '{section_name}'
                        AND PTS."projectTypeId" = {project_type_id}"""
    print("#"*50 + " QUERY " + "#"*50)
    print(f"{query}")
    section_fields = pd.read_sql(query,conn)
    ordered_section_map_dataframe = section_fields.rename({
            "fieldSelector" : "field_selector",
            "name" : "field_name",
            "projectTypeId" : "customFieldType"
        }, axis=1,inplace=False)
    
    return section_fields




def generate_sorted_df(order_file_txt, filtered_template):
    with open(r"{0}".format(order_file_txt)) as file:
        lines = ", ".join(file.readlines())
        

    def find_field_name_location(field_name):
        if lines.find(field_name) == -1:
            print(f'field: {field_name}, no match')
        return lines.find(field_name)

    filtered_template["order_in_ui"] = filtered_template.apply(
            lambda x: find_field_name_location(x["field_name"]), axis=1
        )

    sorted_map_template_df = filtered_template.sort_values(
            by=["order_in_ui"], ascending=True
        )
    
    top = sorted_map_template_df[sorted_map_template_df["order_in_ui"] >= 0]
    bottom = sorted_map_template_df[sorted_map_template_df["order_in_ui"] < 0]
    map_template_df = pd.concat([top, bottom])

    return map_template_df


class SQLScriptGenerator:
    def __init__(
        self,
        column_name,
        column_name_left_join_on,
        field_type,
        original_table_name,
        contacts_legacy_table_name,
    ) -> dict:
        self.column_name = column_name
        self.column_name_left_join_on = column_name_left_join_on
        self.field_type = field_type
        self.original_table_name = original_table_name
        self.contacts_legacy_table_name = contacts_legacy_table_name
        self.return_values()

    def return_values(self):
        if self.field_type == "PersonLink":
            return self.create_person_link_objects_sentence()
        elif self.field_type == "PersonList":
            return self.create_person_list_sentence_objects()
        elif self.field_type == "Boolean":
            return self.create_boolean_column_sentence()
        else:
            return self.create_person_coulumn_sentence()

    def create_boolean_column_sentence(self):
        boolean_coulumn_sentence = f"""COALESCE( CASE cs."{self.column_name}" WHEN TRUE THEN 'Yes' WHEN FALSE THEN 'No' END, null) as "{self.column_name}" """
        return {"columns_definition_sentences": boolean_coulumn_sentence}

    def create_person_coulumn_sentence(self):
        person_coulumn_sentence = f"""cs."{self.column_name}" """
        return {"columns_definition_sentences": person_coulumn_sentence}

    def create_person_link_objects_sentence(self):
        person_link_column_sentence = f"""cs."{self.column_name}", (SELECT "fullName" from {self.contacts_legacy_table_name} where {self.contacts_legacy_table_name}."personId" = cs."{self.column_name}") as "{self.column_name}_fullName" """
        return {"columns_definition_sentences": person_link_column_sentence}

    def create_person_list_sentence_objects(self):
        person_list_table_name = f"{self.column_name}PersonListTable"
        person_list_temp_table_creation_sentence = f""" {person_list_table_name} AS (
        SELECT
            "{self.column_name}" AS "{self.column_name}_key",
            STRING_AGG("fullName", ',') AS "fullName"
        FROM
            ( -- Extract non-null
                SELECT DISTINCT
                    C."{self.column_name}",
                    CN."fullName"
                FROM
                    {self.original_table_name} C
                    JOIN {self.contacts_legacy_table_name} CN ON CN."personId" = ANY (C."{self.column_name}")
                WHERE
                    C."{self.column_name}" IS NOT NULL
            ) {self.column_name}_TABLE
        GROUP BY
            "{self.column_name}") """
        person_list_column_sentence = f""" cs."{self.column_name}",{self.column_name}."fullName" as "{self.column_name}_names_fullName" """
        person_list_join_sentence = f"""left join {person_list_table_name} as {self.column_name} on {self.column_name}."{self.column_name}_key" = cs."{self.column_name}" """

        return {
            "temporal_tables_sentences": person_list_temp_table_creation_sentence,
            "columns_definition_sentences": person_list_column_sentence,
            "join_sentences": person_list_join_sentence,
        }
    def create_datetime_from_timestamp_sentence(self):
         datetime_from_timestamp = f"""to_timestamp(cs."{self.column_name}"/1000)::timestamp as "{self.column_name}" """
         return {"columns_definition_sentences": datetime_from_timestamp}


def create_query_items(
    map_file,
    left_join_on="personId",
    original_table_name="",
    contacts_legacy_table_name="",
    additional_columns_to_show="",
):

    temporal_tables_sentences = []
    columns_definition_sentences = []
    join_sentences = []

    for row in map_file:

        query_object = SQLScriptGenerator(
            column_name=row["field_selector"],
            column_name_left_join_on=left_join_on,
            field_type=row["customFieldType"],
            original_table_name=original_table_name,
            contacts_legacy_table_name=contacts_legacy_table_name,
        )
        query_values = query_object.return_values()
        
        if query_values.get("temporal_tables_sentences"):
            if len(temporal_tables_sentences) == 0:
                temporal_tables_sentences.append(
                    "WITH " + query_values.get("temporal_tables_sentences")
                )
            else:
                temporal_tables_sentences.append(
                    query_values.get("temporal_tables_sentences")
                )
        if query_values.get("columns_definition_sentences"):
            if len(columns_definition_sentences) == 0:
                if additional_columns_to_show:
                    additional_columns_to_show = ", ".join(
                        [
                            f'cs."{column_name}"'
                            for column_name in additional_columns_to_show
                        ]
                    )
                    columns_definition_sentences.append(additional_columns_to_show)
                if row["sectionType"] == "collections":
                    columns_definition_sentences.append(
                        MANDATORY_COLUMNS_FOR_COLLECTIONS
                    )
                elif row["sectionType"] == "form":
                    columns_definition_sentences.append(MANDATORY_COLUMNS_FOR_FORM)
                #finally
                columns_definition_sentences.append(
                        query_values.get("columns_definition_sentences")
                    )
                
            else:
                columns_definition_sentences.append(
                    query_values.get("columns_definition_sentences")
                )
        if query_values.get("join_sentences"):
            join_sentences.append(query_values.get("join_sentences"))

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
    FROM {original_table_name} AS cs
    {join_sentences}
    """

    return query_result


def generate_query(template_file, schema_df, additional_columns_to_show):

    variables, objects, query_variables = read_template(template_file)
    output_file = variables["query_output_name"]["default"]

    if variables["construct_query"]["default"]:
        schema = schema_df.to_dict(orient="records")
        query_result = create_query_items(
            map_file=schema,
            additional_columns_to_show=additional_columns_to_show,
            original_table_name=variables["original_table_name"]["default"],
            contacts_legacy_table_name=variables["contacts_legacy_table"]["default"],
        )

        with open(output_file, "w") as f:
            f.write(query_result)

    replaced_object = replace_variables(variables, objects)

    with open(output_file, "w") as f:
        f.write(replaced_object)

    print(f"Output saved to {output_file}")


if __name__ == "__main__":
    # Obtener argumentos de la línea de comandos
    if len(sys.argv) < 1:
        print("Uso: python template_file=<full_template_file_path>")
        sys.exit(1)

    template_file = r"{0}".format(sys.argv[1])


    variables, objects, query_variables = read_template(template_file)

    section_name = variables["section_name"]["default"]
    order_file_txt = variables.get("order_file_txt",{}).get("default")
    csv_template_location = variables["csv_template_location"]["default"]

    additional_columns_to_show = (
        read_template(template_file)[0]
        .get("additional_columns_to_show", {})
        .get("default")
    )

    schema_df = read_schema(variables=variables
                            ,csv_template_location=csv_template_location
                            ,section_name=section_name
                            ,order_file_txt=order_file_txt)

    generate_query(
        template_file=template_file,
        schema_df=schema_df,
        additional_columns_to_show=additional_columns_to_show,
    )
