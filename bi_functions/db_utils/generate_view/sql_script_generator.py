class SQLScriptGenerator:
    def __init__(
        self,
        field_selector,
        field_selector_left_join_on,
        field_name,
        field_type,
        original_table_name,
        contact_fields,
        contacts_legacy_table_name,
    ) -> dict:
        self.field_selector = field_selector
        self.field_selector_left_join_on = field_selector_left_join_on
        self.field_name = field_name
        self.field_type = field_type
        self.original_table_name = original_table_name
        self.contacts_legacy_table_name = contacts_legacy_table_name
        self.contact_fields = contact_fields
        self.fieldset = {
            1312: "",
            1313: "",
            1314: "",
            2664: "Professional Information: ",
            2805: "Death Details: ",
            2806: "Internal: ",
            2807: "Contact Provided Data to Update their Contact Card: ",
            1316: "",
            2786: "Do not use these fields - Old Data: ",
            2797: "People Finder Results: ",
            2798: "Additional Information: ",
            2810: "",
            2811: "",
        }

        self.return_values()

    def return_values(self):
        if self.field_type == "PersonLink":
            return self.create_person_link_sentence()
        elif self.field_type == "PersonList":
            return self.create_person_list_sentence_objects()
        elif self.field_type == "Boolean":
            return self.create_boolean_column_sentence()
        elif self.field_type in [
            "DocList",
            "MultiSelectList",
            "StringList",
            "MultiDocGen",
            "ProjectLinkList",
        ]:
            return self.create_array_count_column()
        elif self.field_type == "Deadline":
            return self.create_deadline_column()
        else:
            return self.create_column_sentence()

    """ 'PersonLink' : PersonId or Null - Done. Returning full name - extended or null, plus additional columns specified in "contacts_fields"
    'Dropdown' : It only takes one value or Null - Regular column. No transformation needed
    'DocList' : Null or array of numeric value - Return same column without the braces, plus count
    'PersonList' : Done
    'Doc' : Null or document id - Regular column. No transformation needed
    'ActionButton' : Null so far - Regular column. No transformation needed
    'OutlawTemplate' : 
    'MultiSelectList' : Array between braces and separated by commas, or null - Return same column without the braces, plus count
    'StringList' : Array between braces and separated by commas, or null - Return same column without the braces, plus count
    'MultiDocGen' : Array between braces and separated by commas, or null - Return same column without the braces, plus count
    'Deadline' : JSON between braces. "doneDate" and "dateValue"
    'ProjectLinkList' : Array with projectId separated by commas - Return same column without the braces, plus count """

    """
    'PersonLink' : PersonId or Null
    'Dropdown' : It only takes one value or Null
    'DocList' : Null or array of numeric value
    'PersonList' : 
    'Doc' : Null or document id
    'ActionButton' : Null so far
    'OutlawTemplate' : 
    'MultiSelectList' : Array between braces and separated by commas, or null
    'StringList' : Array between braces and separated by commas, or null
    'MultiDocGen' : Array between braces and separated by commas, or null
    'Deadline' : JSON between braces. "doneDate" and "dateValue"
    'ProjectLinkList' : Array with projectId separated by commas

    Exceptions:
    Marketing Source - Truncated
    DofNemailSubmission - is actually dofNemailSubmission


    """

    def create_person_link_sentence(self):
        """
        Function to add the personId and additional information from a contact card

        Args:
            - self: contains the field_selector(personId)
            - fields_to_add(dict): Dict with the mapping of field_selector and field_name of the new field
        """

        # Add the "full name - extended" column
        person_link_column_sentence = f"""cs."{self.field_selector}" as "{self.field_name.lower()} person id",
            current.gen_full_name_extended(cs."{self.field_selector}") AS "{self.field_name.lower()} full name - extended" """
        if False:
            person_link_column_sentence = f"""cs."{self.field_selector}" as "{self.field_name.lower()} person id",
                (SELECT CONCAT_WS(' ',TRIM("prefix"),TRIM("firstName"),TRIM("middleName"),TRIM("lastName"),TRIM("suffix"))
                FROM {self.contacts_legacy_table_name} WHERE {self.contacts_legacy_table_name}."personId" = cs."{self.field_selector}")
                AS "{self.field_name.lower()} full name - extended" """

        # Add additional columns for said contact card
        if len(self.contact_fields) != 0:
            for index, additional_field in self.contact_fields.iterrows():
                if additional_field["related_field_selector"] == "addresses":
                    for i in range(1, 5):
                        person_link_column_sentence += f""",
                        (SELECT "addresses" -> {i-1} ->> 'fullAddress'
                        FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}" WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}")
                        AS "{self.field_name.lower()} address {i}" """
                        person_link_column_sentence += f""",
                        (SELECT "addresses" -> {i-1} ->> 'line1'
                        FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}" WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}")
                        AS "{self.field_name.lower()} address {i} line 1" """
                        person_link_column_sentence += f""",
                        (SELECT "addresses" -> {i-1} ->> 'line2'
                        FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}" WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}")
                        AS "{self.field_name.lower()} address {i} line 2" """
                        person_link_column_sentence += f""",
                        (SELECT "addresses" -> {i-1} ->> 'city'
                        FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}" WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}")
                        AS "{self.field_name.lower()} address {i} city" """
                        person_link_column_sentence += f""",
                        (SELECT "addresses" -> {i-1} ->> 'state'
                        FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}" WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}")
                        AS "{self.field_name.lower()} address {i} state" """
                        person_link_column_sentence += f""",
                        (SELECT "addresses" -> {i-1} ->> 'postalCode'
                        FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}" WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}")
                        AS "{self.field_name.lower()} address {i} zip" """
                elif additional_field["related_field_selector"] == "phones":
                    person_link_column_sentence += f""",
                        (SELECT ARRAY_TO_STRING(ARRAY_AGG(elements ->> 'number'), ',') AS "phones"
	                    FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}", JSONB_ARRAY_ELEMENTS("phones") AS elements
	                    WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}"
	                    GROUP BY current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId")
                        AS "{self.field_name.lower()} phones" """
                elif additional_field["related_field_selector"] == "emails":
                    person_link_column_sentence += f""",
                        (SELECT ARRAY_TO_STRING(ARRAY_AGG(elements ->> 'number'), ',') AS "emails"
	                    FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}", JSONB_ARRAY_ELEMENTS("emails") AS elements
	                    WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}"
	                    GROUP BY current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId")
                        AS "{self.field_name.lower()} emails" """
                elif additional_field["tab_title"] != "":
                    if additional_field["value"] == "bool":
                        person_link_column_sentence += f""",
                        (SELECT COALESCE(CASE "{additional_field["related_field_selector"]}" WHEN TRUE THEN 'Yes' WHEN FALSE THEN 'No' END, null)
                        """
                    elif additional_field["value"] == "object":
                        person_link_column_sentence += f""",
                        (SELECT ARRAY_TO_STRING("{additional_field["related_field_selector"]}",',')
                        """
                    else:
                        person_link_column_sentence += f""",
                        (SELECT "{additional_field["related_field_selector"]}" """

                    person_link_column_sentence += f"""
                    FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}" WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}")
                    AS "{self.field_name.lower()}'s {additional_field["tab_title"].lower()}: {self.fieldset[additional_field["fieldset_id"]].lower()}{additional_field["contactFieldName"].lower()}" """
                else:
                    if additional_field["value"] == "bool":
                        person_link_column_sentence += f""",
                        (SELECT COALESCE(CASE "{additional_field["related_field_selector"]}" WHEN TRUE THEN 'Yes' WHEN FALSE THEN 'No' END, null)
                        """
                    elif additional_field["value"] == "object":
                        person_link_column_sentence += f""",
                        (SELECT ARRAY_TO_STRING("{additional_field["related_field_selector"]}",',')
                        """
                    else:
                        person_link_column_sentence += f""",
                        (SELECT "{additional_field["related_field_selector"]}" """
                    person_link_column_sentence += f"""                
                    FROM current."fvdw_Contact_{additional_field["contact_card_section_id"]}" WHERE current."fvdw_Contact_{additional_field["contact_card_section_id"]}"."personId" = cs."{self.field_selector}")
                    AS "{self.field_name.lower()} {additional_field["contactFieldName"].lower()}" """

        return {"columns_definition_sentences": person_link_column_sentence}

    def create_boolean_column_sentence(self):
        boolean_column_sentence = f"""COALESCE( CASE cs."{self.field_selector}" WHEN TRUE THEN 'Yes' WHEN FALSE THEN 'No' END, null) AS "{self.field_name.lower().replace('"',"")}" """
        return {"columns_definition_sentences": boolean_column_sentence}

    def create_column_sentence(self):
        column_sentence = f"""cs."{self.field_selector}" AS "{self.field_name.lower().replace('"', "")}" """
        return {"columns_definition_sentences": column_sentence}

    def create_person_list_sentence_objects(self):
        person_list_table_name = f"{self.field_selector}PersonListTable"
        person_list_temp_table_creation_sentence = f""" {person_list_table_name} AS (
        SELECT
            "{self.field_selector}" AS "{self.field_selector}_key",
            STRING_AGG("fullName", ',') AS "fullName"
        FROM
            ( -- Extract non-null
                SELECT DISTINCT
                    C."{self.field_selector}",
                    CN."fullName"
                FROM
                    {self.original_table_name} C
                    JOIN {self.contacts_legacy_table_name} CN ON CN."personId" = ANY (C."{self.field_selector}")
                WHERE
                    C."{self.field_selector}" IS NOT NULL
            ) {self.field_selector}_TABLE
        GROUP BY
            "{self.field_selector}") """
        person_list_column_sentence = f""" cs."{self.field_selector}",{self.field_selector}."fullName" as "{self.field_name.lower()} names full name" """
        person_list_join_sentence = f"""left join {person_list_table_name} as {self.field_selector} on {self.field_selector}."{self.field_selector}_key" = cs."{self.field_selector}" """

        return {
            "temporal_tables_sentences": person_list_temp_table_creation_sentence,
            "columns_definition_sentences": person_list_column_sentence,
            "join_sentences": person_list_join_sentence,
        }

    def create_array_count_column(self):
        # Return the same column, plus a count of the amount of elements present in the array
        array_count_column_sentence = f"""ARRAY_TO_STRING(cs."{self.field_selector}",',') AS "{self.field_name.lower()}",
        ARRAY_LENGTH(cs."{self.field_selector}", 1) AS "count of {self.field_name.lower()}"
        """
        return {"columns_definition_sentences": array_count_column_sentence}

    def create_deadline_column(self):
        # Return only date and done dates
        deadline_column_sentence = f"""cs."{self.field_selector}_DateValue" AS "{self.field_name.lower()} due",
        cs."{self.field_selector}_DoneDate" AS "{self.field_name.lower()} done"
        """
        return {"columns_definition_sentences": deadline_column_sentence}

    def read_template(self, template_file: str):
        """
        Read the YAML template and extract variables and objects.

        Args:
            template_file (str): Path to the YAML file specific for each section

        Returns:
            variables: Dictionary with the Variables attribute from the YAML file
            objects: List with the Objects attribute from the YAML file
            query_variables: Dictionary with the QueryVariables attribute from the YAML file
        """
        with open(template_file, "r") as f:
            template_data = yaml.safe_load(f)

        variables = template_data.get("Variables", {})
        objects = template_data.get("Objects", [])
        query_variables = template_data.get("QueryVariables", {})

        return variables, objects, query_variables

    def replace_variables(self, variables, objects):
        """
        Replace variables in objects.
        """

        objects = "\n".join(objects.values())

        for var_name, value in variables.items():

            if isinstance(value.get("type"), str) and value.get("type").startswith(
                "file"
            ):
                # If the value is a file, read its content
                filename = value.get("default")
                with open(filename, "r") as f:
                    file_content = f.read()
                objects = objects.replace(f"{{{{{var_name}}}}}", file_content + "\n")
            else:
                objects = objects.replace(
                    f"{{{{{var_name}}}}}", str(value.get("default"))
                )
        return objects
