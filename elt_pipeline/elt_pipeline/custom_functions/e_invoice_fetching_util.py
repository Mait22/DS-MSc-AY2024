from lxml import etree
import warnings
import pandas as pd


def check_if_valid_xml(path_to_file: str,
                       xpath_query: str = "/E_Invoice",
                       criteria_type: str = "tag",
                       criteria_val: str = "E_Invoice",
                       show_warnings: bool = False):
    """
    :param path_to_file: path to serialized xml file
    :param xpath_query: xpath query to test if file is valid XML
    :param criteria_type: tag or value to decide if XML is valid
    :param criteria_val: if XMl is parsable, use this value to decide if XML is valid,
                         criteria is parsed against xpath query
    :param show_warnings: print and raise warning if True

    :return: in case of valid XMl return str dump of XML
    """
    try:
        tree = etree.parse(path_to_file)

        try:
            assert len(tree.xpath(xpath_query)) == 1
            item = tree.xpath(xpath_query)[0]

            if criteria_type == "tag":
                if item.tag == criteria_val:
                    return etree.tostring(tree).decode()
                else:
                    return None
            elif criteria_type == "value":
                if item.text == criteria_val:
                    return etree.tostring(tree).decode()
                else:
                    return None
        except Exception as e:
            if show_warnings:
                print(e)
                warnings.warn(f"Warning - {path_to_file} - error in parsing step")
            return None

    except Exception as e:
        if show_warnings:
            print(e)
            warnings.warn(f"Warning - {path_to_file} - is not a valid XML file")
        return None


def parse_single_val_from_xml(tree: dict,
                              xpath_query: str):
    """
    :param tree: XML as str UTF8 decoded dump
    :param xpath_query: XPath query

    :return: if value present in the XML return str value, else None
    """

    try:
        tree = etree.fromstring(tree[list(tree.keys())[0]])

        try:
            item = tree.xpath(xpath_query)
            assert len(item) == 1
            return item[0].text
        except Exception as e:
            print(e)
            warnings.warn(f"Warning - {list(tree.keys())[0]} - returned error")
            return None

    except Exception as e:
        print(e)
        warnings.warn(f"Warning - {list(tree.keys())[0]} - is not a valid XML file")
        return None


def parse_xml_to_dicts(tree: dict,
                       item_entry_path: str = "/E_Invoice/Invoice/InvoiceItem/InvoiceItemGroup/ItemEntry",
                       carry_over_attributes: list[dict] = None
                       ):
    """
    :param tree: XML as str UTF8 decoded dump
    :param item_entry_path: Path to root
    :param carry_over_attributes: in case of parent node having no value but has a tag attribute, and parent tag matches
                                 dict key and attribute matches dict value extend child tag name by attribute name

    :return: dict of values parsed from XML, breath first search, two levels deep

    1. Parses xml from str dump
    2. Creates dictionary from xml data starting from a list of root nodes specified by <<item_entry_path>>
       Note: fetching is carried out 1 + 1 level deep using depth first loops
    3. Attribute from parent node is carried over to child node's dict value key if specified in carry_over_attributes
       Note: parameter assumes {<<parent leaf name>>: <<parent leaf attribute>>}
    """

    if carry_over_attributes is None:
        carry_over_attributes = [{'ItemReserve': "extensionId"}]

    try:
        tree = etree.fromstring(tree[list(tree.keys())[0]])
        item_entries = tree.xpath(item_entry_path)
        result_entries = []

        # Fetch items 1 + 1 levels deep from XML tree
        for i_e in item_entries:
            res_e = {}

            # Level 1 - root children
            for e in i_e.iter():

                if e.tag in res_e.keys():
                    if e.text is not None:
                        if e.text.strip() != '':
                            res_e[e.tag].append(e.text.strip())
                else:
                    if e.text is not None:
                        if e.text.strip() != '':
                            res_e[e.tag] = [e.text.strip()]

                # Level 2 - leafs of child's
                for ee in e.iter():

                    # Extend by attribute of parent leaf
                    extend_by_attrib = False
                    if (e.text is None or e.text.strip() == '') and len(list(e.attrib.keys())) > 0:

                        for coa in carry_over_attributes:
                            k = list(coa.keys())[0]
                            v = coa[k]

                            if e.tag == k and v in list(e.attrib.keys()):
                                extend_by_attrib = True
                                break

                    # No value key extension by attribute
                    if extend_by_attrib:
                        res_key = " - ".join([ee.tag, e.attrib[v]])
                        if res_key in res_e.keys():
                            if ee.text is not None:
                                if ee.text.strip() != '':
                                    res_e[res_key].append(ee.text.strip())
                        else:
                            if ee.text is not None:
                                if ee.text.strip() != '':
                                    res_e[res_key] = [ee.text.strip()]

                    else:
                        if ee.tag in res_e.keys():
                            if ee.text is not None:
                                if ee.text.strip() != '':
                                    res_e[ee.tag].append(ee.text.strip())
                        else:
                            if ee.text is not None:
                                if ee.text.strip() != '':
                                    res_e[ee.tag] = [ee.text.strip()]

                for key in res_e.keys():
                    res_e[key] = list(dict.fromkeys(res_e[key]))

            result_entries.append(res_e)
        return result_entries

    except Exception as e:
        print(e)
        warnings.warn(f"Warning - {list(tree.keys())[0]} - error in fetching XML file")
        return None


def get_val_f_dict(dictionary: dict,
                   val_key: str,
                   allow_append: bool = False,
                   allow_list_return: bool = False,
                   missing_value_flag: str = "Missing",
                   multiple_value_flag: str = "Multiple values",
                   missing_tag_flag: str = "Missing tag"):
    """
    :param dictionary: xml tree dump in string format, assumes check_if_valid_xml() pre-check
    :param val_key: XML key to search for
    :param allow_append: if key is present multiple times, allow for appending individual values
    :param allow_list_return: if key is present multiple times, allow for list return of individual values
    :param missing_value_flag: default err return value
    :param multiple_value_flag: default err return value
    :param missing_tag_flag: default err return value

    :return: value from dict if found; else missing value or error flag
    """

    if val_key in dictionary.keys():

        val = dictionary[val_key]

        if len(val) == 0:
            warnings.warn(f"Warning - missing {missing_value_flag} - key: {val_key}")
            return missing_value_flag
        elif len(val) > 1:
            if not allow_append and not allow_list_return:
                warnings.warn(f"Warning - multiple {multiple_value_flag} - key: {val_key}")
                return multiple_value_flag
            elif allow_append:
                return '; '.join(val)
            elif not allow_append and allow_list_return:
                return val
        elif len(val) == 1:
            return val[0]
    else:
        return missing_tag_flag


def xml_to_df(tree_str: dict,
              fetch_dict: list[dict] = None,

              missing_tag_flag: str = "Missing tag",
              missing_value_flag: str = "Missing",
              criteria_mismatch_flag: str = "Criteria mismatch",
              other_error_flag: str = "Error"
              ):
    """
    :param tree_str: xml tree dump in string format, assumes check_if_valid_xml() pre-check
    :param fetch_dict: parametrization of queries: xpath and xpath -> dict
                       keys:
                            - column_name: name of the column in output df
                            - val_key: value to search for, xpath if single value; dict key if multi leaf node
                            - is_single_val: if true, use xpath query; if false convert to dict and search value
                            - allow_append: if multiple values for the same dict key are returned, try to reduce
                                            the return list (keep unique values and return value only if one value)
                            - allow_list_val: if true return value even if key has multi item list of values
                            - list_select_criteria: TO-DO
    :param missing_tag_flag: returned if tag is not present
    :param missing_value_flag: returned if tag present but no value
    :param criteria_mismatch_flag: returned if value and criteria arrays do not match
    :param other_error_flag: in case of other error, return this flag

    :return: returns pandas df of fetched values
    """

    # Set default arguments for fetch dict
    if fetch_dict is None:
        fetch_dict = [
            {'column_name': ["SellerName"], 'val_key': ["/E_Invoice/Invoice/InvoiceParties/SellerParty/Name"],
             'is_single_val': True, 'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["RegNumber"], 'val_key': ["/E_Invoice/Invoice/InvoiceParties/SellerParty/RegNumber"],
             'is_single_val': True, 'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["InvoiceNumber"], 'val_key': ["/E_Invoice/Invoice/InvoiceInformation/InvoiceNumber"],
             'is_single_val': True, 'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["InvoiceDate"], 'val_key': ["/E_Invoice/Invoice/InvoiceInformation/InvoiceDate"],
             'is_single_val': True, 'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["Amount"], 'val_key': ["ItemAmount"], 'is_single_val': False,
             'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["ItemUnit"], 'val_key': ["ItemUnit"], 'is_single_val': False, 'allow_append': False,
             'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["Description"], 'val_key': ["Description"], 'is_single_val': False, 'allow_append': False,
             'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["AddRate"], 'val_key': ["AddRate"], 'is_single_val': False, 'allow_append': False,
             'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["ItemPrice"], 'val_key': ["ItemPrice"], 'is_single_val': False, 'allow_append': False,
             'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["SumBeforeVAT"], 'val_key': ["SumBeforeVAT"], 'is_single_val': False,
             'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["VATSum"], 'val_key': ["VATSum"], 'is_single_val': False, 'allow_append': False,
             'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["ItemTotal"], 'val_key': ["ItemTotal", "ItemSum"], 'is_single_val': False,
             'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["SellerProductId"], 'val_key': ["SellerProductId"], 'is_single_val': False,
             'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["EAN"], 'val_key': ["EAN"], 'is_single_val': False, 'allow_append': False,
             'allow_list_val': False, 'list_select_criteria': None},

            {'column_name': ["ServicePoint"], 'val_key': ["InformationContent - notes"], 'is_single_val': False,
             'allow_append': False, 'allow_list_val': False, 'list_select_criteria': None}
        ]
    try:
        value_dicts = parse_xml_to_dicts(tree_str)

        # Create return DF
        return_df = {}
        column_names = []
        for cn in fetch_dict:
            column_names.append(cn['column_name'][0])
            return_df[cn['column_name'][0]] = []

        # Iterate over item entries
        for row, dic in enumerate(value_dicts):
            for col, cn in enumerate(column_names):

                # Single value fetching
                if fetch_dict[col]['is_single_val']:
                    return_df[cn].append(parse_single_val_from_xml(tree_str, fetch_dict[col]['val_key'][0]))

                # Get single value
                for i, val_key in enumerate(fetch_dict[col]['val_key']):
                    if not fetch_dict[col]['is_single_val'] and not fetch_dict[col]['allow_append'] and not \
                            fetch_dict[col]['allow_list_val'] and not fetch_dict[col]['list_select_criteria']:
                        val = get_val_f_dict(dic, val_key, fetch_dict[col]['allow_append'],
                                             fetch_dict[col]['allow_list_val'])

                        if val is not None and val not in [missing_tag_flag, missing_value_flag]:
                            return_df[cn].append(val)
                            break
                        elif (val is None or val in [missing_tag_flag, missing_value_flag]) and i + 1 < len(
                                fetch_dict[col]['val_key']):
                            next
                        elif val in [missing_tag_flag, missing_value_flag]:
                            return_df[cn].append(val)
                        else:
                            return_df[cn].append(other_error_flag)

                # Get single value from list of values
                for i, val_key in enumerate(fetch_dict[col]['val_key']):
                    if not fetch_dict[col]['is_single_val'] and not fetch_dict[col]['allow_append'] and \
                            fetch_dict[col]['allow_list_val'] and not fetch_dict[col]['list_select_criteria'] is None:
                        val = get_val_f_dict(dic, val_key, fetch_dict[col]['allow_append'],
                                             fetch_dict[col]['allow_list_val'])
                        val_criteria = get_val_f_dict(dic, fetch_dict[col]['list_select_criteria']['crit_col'],
                                                      fetch_dict[col]['allow_append'],
                                                      fetch_dict[col]['allow_list_val'])

                        if val is None or val_criteria is None or val == missing_tag_flag or val_criteria == missing_tag_flag:
                            return_df[cn].append(missing_tag_flag)
                        elif not fetch_dict[col]['list_select_criteria']["list_order_val"] in val_criteria:
                            return_df[cn].append(criteria_mismatch_flag)
                        elif val == missing_value_flag or val_criteria == missing_value_flag:
                            return_df[cn].append(missing_value_flag)
                        elif len(val) != len(val_criteria):
                            return_df[cn].append(criteria_mismatch_flag)
                        else:
                            crit_idx = val_criteria.index(fetch_dict[col]['list_select_criteria']['list_order_val'])
                            return_df[cn].append(val[crit_idx])

        return_df = pd.DataFrame.from_dict(return_df)
        return_df["Filename"] = list(tree_str.keys())[0]

        return return_df
    except Exception as e:
        print(e)
        print(list(tree_str.keys())[0])
        return None


def suggest_to_clean(df: pd.DataFrame,
                     to_keep_c_name: str = "ToKeep",
                     missing_tag: str = "Missing tag",
                     criteria_columns: list = None,
                     update_descriptions: bool = True):
    """

    :param df: input data
    :param to_keep_c_name: column which indicates if the row should be kept after data cleaning 
    :param missing_tag: missing tags in indput data
    :param criteria_columns: columns across which to do data cleaning, i.e. rm rows if all crit values have missing_tag val
    :param update_descriptions: try to update the description column with info from previous or preceding deleted rows
    :return: input df with updated data 
    """

    if criteria_columns is None:
        criteria_columns = ["ItemUnit", "ItemPrice", "Amount"]

    df[to_keep_c_name] = df[criteria_columns].ne(missing_tag).all(axis=1)
    df = df.reset_index()

    if update_descriptions:
        desc = None
        to_update = []

        for i, r in df.iterrows():
            if not r['ToKeep']:
                desc = r["Description"]
            elif desc is None:
                df.append(i)
            elif desc is not None:
                if len(to_update) > 0:
                    for ii in to_update:
                        df.loc[ii, ['Description']] = df.loc[ii, ['Description']] + "; " + desc
                    to_update.clear()
                df.loc[i, ['Description']] = df.loc[i, ['Description']] + "; " + desc

    return df


def generate_surrogate_key(df: pd.DataFrame,
                           key_col_name: str = "SurrogateKey",
                           key_col_sources: list = None,
                           sep: str = "_"):
    """

    :param df: input data
    :param key_col_name: new key col's name
    :param key_col_sources: list of columns to concatenate to create a new key col
    :param sep: concat separator 

    :return: input df with new key col added
    """

    if key_col_sources is None:
        key_col_sources = ["RegNumber", "InvoiceNumber", "Filename", "InvoiceDate"]

    df = df.reset_index()
    df[key_col_name] = df.index.map(str) + "_" + df[key_col_sources].apply(lambda row: sep.join(row.values.astype(str)), axis=1)

    return df