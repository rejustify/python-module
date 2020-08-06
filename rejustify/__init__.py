import os
import copy
import requests
import pandas as pd
import json

# global variables
rejustify_main_url = os.environ.get('rejustify_main_url') or 'https://api.rejustify.com'
rejustify_proxy_url = os.environ.get('rejustify_proxy_url') or None
rejustify_proxy_port = os.environ.get('rejustify_proxy_port') or None
rejustify_learn = os.environ.get('rejustify_learn') or True
rejustify_token = os.environ.get('rejustify_token') or None
rejustify_email = os.environ.get('rejustify_email') or None


def setCurl(main_url=None, proxy_url=None, proxy_port=None, learn=None):
    """

    This command stores the connection details into memory to be easier accessed by rejustify Python module.

    For more details and to register an account visit https://rejustify.com.

    Examples:
        rejustify.setCurl()
        rejustify.setCurl(learn = False)
        rejustify.setCurl(proxy_url = "PROXY_ADDRESS", proxy_port = 8080)

    Attributes:
        main_url(str): Main address for rejustify API calls. Default is set to
            https://api.rejustify.com, but depending on the customer needs, the address
            may change.
        proxy_url(str): Address of the proxy server.
        proxy_port(str): Port for communication with the proxy server.
        learn(bool): Enable AI learning in all API calls by setting learn=True. You can also
            specify the learn option in the relevant functions directly.
    """

    global rejustify_main_url, rejustify_proxy_url, rejustify_proxy_port, rejustify_learn

    # error handling
    if main_url is not None and not isinstance(main_url, str):
        raise ValueError("`main_url` parameter must be a string")
    if proxy_url is not None and not isinstance(proxy_url, str):
        raise ValueError("`proxy_url` parameter must be a string")
    if proxy_port is not None and not isinstance(proxy_port, int):
        raise ValueError("`proxy_port` parameter must be an integer")
    if learn is not None and not isinstance(learn, bool):
        raise ValueError("`learn` parameter must be True/False")

    # assign values
    if main_url is not None:
        rejustify_main_url = main_url
    if proxy_url is not None:
        rejustify_proxy_url = proxy_url
    if proxy_port is not None:
        rejustify_proxy_port = proxy_port
    if learn is not None:
        rejustify_learn = learn

    # consistency checks
    if(rejustify_proxy_url is ''):
        rejustify_proxy_url = None
    if(rejustify_proxy_port is ''):
        rejustify_proxy_port = None
    if(type(rejustify_learn) is not bool):
        rejustify_learn = True


def getCurl():
    """

    This command summarizes the current curl settings.

    Examples:
        rejustify.getCurl()
    """

    print('Main API address: ' + rejustify_main_url)
    print('Proxy address: ' + (rejustify_proxy_url if rejustify_proxy_url is not None else 'No proxy address'))
    print('Proxy port: ' + (rejustify_proxy_port if rejustify_proxy_port is not None else 'No proxy port'))
    print('Enable learning: ' + 'Yes' if rejustify_learn is True else 'No')


def register(token=None, email=None):
    """

    This command stores the account details into memory to be easier accessed by
    rejustify module. The email must correspond to the token that was assigned to it.

    To register an account visit https://rejustify.com.

    Examples:
        register(token = "YOUR_TOKEN", email = "YOUR_EMAIL")

    Attributes:
        token(str): API token.
        email(str): E-mail address for the account.
    """

    global rejustify_token, rejustify_email

    # error handling
    if token is not None and not isinstance(token, str):
        raise ValueError("`token` parameter must be a string")
    if email is not None and not isinstance(email, str):
        raise ValueError("`email` parameter must be a string")

    # assign values
    if token is not None:
        rejustify_token = token
    if email is not None:
        rejustify_email = email

    # consistency checks
    if(rejustify_token is ''):
        rejustify_token = None
    if(rejustify_email is ''):
        rejustify_email = None


def analyze(df=None, shape="vertical", inits=1, fast=True,
            sep=",", learn=None, token=None,
            email=None, url=None):
    """

    The function submits the data set to the analyze API endpoint and
    returns the proposed structure of the data. At the current stage
    data set must be rectangular, either vertical or horizontal.

    API recognizes the multi-dimension and multi-line headers, however, for now the Python module supports
    multi-dimension but single line headers. Make sure that the separator doesn't appear in the header values. Also only vertical
    data shape is supported in Python.

    The classification algorithms are applied to the values in the rows/columns if they are not empty, and
    to the headers if the rows/columns are empty. For efficiency reasons only a sample of values in each column is analyzed.
    To improve the classification accuracy, you can ask the API to draw a larger sample by setting fast=False.
    For empty columns the API returns the proposed resources that appear to fit well in the empty spaces given the header
    information and overall structure of df.

    The basic properties are characterized by classes. Currently, the API distinguishes between 6 classes: general,
    geography, unit, time, sector, number. They describe the basic characteristics of the
    values, and they are further used to propose the best transformations and matching methods for data reconciliation. Classes
    are further supported by features, which determine the characteristics in greater detail, such as class geography
    may be further supported by feature country.

    Cleaner contains the basic set of transformations applied to each value in a dimension to retrieve machine-readable
    representation. For instance, values y1999, y2000, ..., clearly correspond to years, however, they will
    be processed much faster if stripped from the initial 'y' character, such as '^y'. Cleaner allows basic regular expressions.

    Finally, format corresponds to the format of the values, and it is particularly useful for time-series operations. Format allows
    the standard date formats.

    The classification algorithm can be substantially improved by allowing it to recall how
    it was used in the past and how well it performed. Parameter learn controls this feature, however, by default it
    is disabled. The information stored by rejustify is tailored to each user individually and it can substantially
    increase the usability of the API. For instance, the proposed provider for empty row/column with header 'gross domestic product'
    is IMF. Selecting another provider, for instance AMECO, will teach the algorithm that for this combination
    of headers and rows/columns AMECO is the preferred provider, such that the next time API is called, there will be
    higher chance of AMECO to be picked by default. To enable learning option in all API calls by default, run
    setCurl(learn = True).

    If learn=True, the information stored by rejustify include (i) the information changed by the user with respect
    to assigned class, feature, cleaner and format, (ii) resources determined by provider,
    table and headers of df, (iii) hand-picked matching values for value-selection. The information will
    be stored only upon a change of values within groups (i-iii).

    Examples:
        # API setup
        setCurl()

        # register token/email
        register(token = "YOUR_TOKEN", email = "YOUR_EMAIL")

        # prepare sample data set
        df = pd.DataFrame()
        df['country'] = ['Italy'] * 4
        df['date'] = pd.date_range('2020-06-01', periods=4).strftime("%Y-%m-%d")
        df['covid cases'] = ""

        # rejustify
        analyze(df)

    Attributes:
        df(DataFrame): The data set to be analyzed. Must be a DataFrame.
        shape(str): It informs the API whether the data set should be read by
            columns (vertical) or by rows (horizontal). The current Python module support only vertical.
        inits(int): It informs the API how many initial rows (or columns in horizontal data), correspond
            to the header description. The default is inits=1.
        fast(bool): Informs the API on how big a sample draw of original data should be. The larger
            the sample, the more precise but overall slower the algorithm. Under the fast=True the API
            samples 5% of data points, under the fast=False option it is 25%. Default is fast=True.
        sep(str): The header can also be described by single field values, separated by a given character
            separator, for instance 'GDP, Austria, 1999'. The option informs the API which separator should
            be used to split the initial header string into corresponding dimensions. The default is sep=','.
        learn(bool): It should be set as True if the user accepts rejustify to track her/his activity
            to enhance the performance of the AI algorithms (it is not enabled by default). To change this option
            for all API calls run setCurl(learn = True).
        token(str): API token. By default read from global variables.
        email(str): E-mail address for the account. By default read from global variables.
        url(url): API url. By default read from global variables.
    """

    # error handling
    if df is not None and not isinstance(df, pd.DataFrame):
        raise ValueError("`df` parameter must be a DataFrame object")
    if df is None:
        raise ValueError("`df` parameter must be a DataFrame object")
    if shape is not "vertical":
        raise ValueError(
            "`shape` parameter must be vertical (horizontal tables are not yet supported in Python)")
    if inits is not None and not isinstance(inits, int):
        raise ValueError("`inits` parameter must be an integer")
    if inits is not None and inits > 1:
        raise ValueError("Currently `inits` can be max 1")
    if fast is not None and not isinstance(fast, bool):
        raise ValueError("`fast` parameter must be True/False")
    if sep is not None and not isinstance(sep, str):
        raise ValueError("`sep` parameter must be a string")
    if len(sep) > 3:
        raise ValueError("`sep` has a maximum of 3 characters")
    if learn is not None and not isinstance(learn, bool):
        raise ValueError("`learn` parameter must be True/False")
    if token is not None and not isinstance(token, str):
        raise ValueError("`token` parameter must be a string")
    if email is not None and not isinstance(email, str):
        raise ValueError("`email` parameter must be a string")
    if url is not None and not isinstance(url, str):
        raise ValueError("`url` parameter must be a string")

    # set global variables
    if learn is None:
        learn = rejustify_learn
    if token is None:
        token = rejustify_token
    if email is None:
        email = rejustify_email
    if url is None:
        url = rejustify_main_url

    # reassign mutable objects
    _df = df.copy()

    # convert DataFrame to array with correct naming
    _df.loc[-1] = _df.columns
    _df.index = _df.index + 1
    _df = _df.sort_index()

    # prepare the payload query
    payload = {}
    payload['data'] = _df.values.tolist()
    payload['userToken'] = token
    payload['email'] = email
    payload['dataShape'] = shape
    payload['inits'] = inits
    payload['fast'] = fast
    payload['sep'] = sep
    payload['dbAllowed'] = learn

    # send request
    if rejustify_proxy_url is not None and rejustify_proxy_port is not None:
        response = requests.post(rejustify_main_url + "/analyze",  data=json.dumps(payload),
                                 headers={'Content-Type': 'application/json'},
                                 proxies={'http': rejustify_proxy_url + ':' + rejustify_proxy_port,
                                          'https': rejustify_proxy_url + ':' + rejustify_proxy_port})
    else:
        response = requests.post(rejustify_main_url + "/analyze",  data=json.dumps(payload),
                                 headers={'Content-Type': 'application/json'})

    try:
        response_json = response.json()
    except ValueError:
        raise ValueError("Invalid response from rejustify (JSON expected)")

    if not response.ok:
        raise ValueError(response_json)

    # output
    try:
        out = pd.DataFrame(response_json['structure'])
    except:
        out = "Consistency error. Check your input parameters."

    return(out)


def adjust(block=None, column=None, id=None, items=None):
    """

     The purpose of the command is to provide a possibly seamless
     way of adjusting blocks used in communication with rejustify API, in particular with the
     fill endpoint. The blocks include: data structure (structure), default values
     (default) and matching keys (keys)). Items may only be deleted for specific matching
     dimensions proposed by keys, for the two other blocks it is possible only to change the relevant
     values.

     Upon changes in structure, the corresponding p_class or p_data will be set to -1.
     This is the way to inform API that the original structure has changed and, if learn
     option is enabled, the new values will be used to train the algorithms in the back end. If learn
     is disabled, information will not be stored by the API but the changes will be recognized in the current API call.

     Examples:
         # API setup
         setCurl()

         # register token/email
         register(token = "YOUR_TOKEN", email = "YOUR_EMAIL")

         # prepare sample data set
         df = pd.DataFrame()
         df['country'] = ['Italy'] * 4
         df['date'] = pd.date_range('2020-06-01', periods=4).strftime("%Y-%m-%d")
         df['covid cases'] = ""

         # rejustify
         st = analyze(df)

         # adjust structures
         st = adjust(st, id = 3, items={'class': 'general',
                                        'feature': None,
                                        'provider': 'REJUSTIFY',
                                        'table': 'COVID-19-ECDC'})

         # endpoint fill
         rdf = fill(df, st)

         # adjust default values
         adjust(rdf['default'], column=3, items={'Time Dimension': 'latest'})

         #adjust keys
         adjust(rdf['keys'], column = 3, items={'id.x': 2, 'id.y': None}) # remove link
         adjust(rdf['keys'], column = 3, items={'id.x': 2, 'id.y': 3}) # change link
         adjust(rdf['keys'], column = 3, items={'id.x': 3, 'id.y': 4}) # add link

     Attributes:
        block(DataFrame, list or dict): A data structure to be changed. Currently supported structures include structure (DataFrame),
            default (dict) and keys (list).
        column(int or list): The data column (or raw in case of horizontal datasets) to be adjusted. Vector values are supported.
        id(int or list): The identifier of the specific element to be changed. Currently it should be only used in structure
            with multi-dimension headers (see analyze() for details).
        items(dict): Specific items to be changed with the new values to be assigned. If the values are set to None,
            the specific item will be removed from the block (only for keys). Items may be multi-valued.
    """

    # error handling
    if block is not None and not isinstance(block, pd.DataFrame) and not isinstance(block, dict) and not isinstance(block, list):
        raise ValueError("`block` parameter must be a DataFrame, a dict or a list object")

    # reassign mutable objects
    index = None
    type = "undefined"

    # define block type
    if isinstance(block, pd.DataFrame):
        if all(elem in block.columns.values.tolist() for elem in {"id", "column", "name", "empty", "class", "feature", "cleaner", "format", "p_class", "provider", "table", "p_data"}):
            type = "structure"
            _block = block.copy()  # shallow copy is enough
    if isinstance(block, dict):
        if all(elem in block.keys() for elem in {"column.id.x", "default"}):
            type = "default"
            _block = copy.deepcopy(block)
    if isinstance(block, list):
        if all(elem in block[0].keys() for elem in {"id.x", "name.x", "id.y", "name.y", "class", "method", "column.id.x", "column.name.x"}):
            type = "keys"
            _block = copy.deepcopy(block)

    # adjust structure
    if type is "structure":
        if items is not None and id is not None:
            if not isinstance(id, list):
                id = [id]
            index = _block.loc[_block['id'].isin(id)].index

        if items is not None and column is not None:
            if not isinstance(column, list):
                column = [column]
            if all(isinstance(elem, int) for elem in column):
                index = _block.loc[_block['column'].isin(column)].index
            else:
                index = _block.loc[_block['name'].isin(column)].index

        if any(elem in items.keys() for elem in {'provider', 'table'}):
            _block.loc[index, 'p_data'] = -1

        if any(elem in items.keys() for elem in {'class', 'feature', 'cleaner', 'format'}):
            _block.loc[index, 'p_class'] = -1

        _block.loc[index, items.keys()] = items.values()

    # adjust default
    if type is "default":
        if items is not None and id is not None:
            if not isinstance(id, list):
                id = [id]
            try:
                index = [_block['column.id.x'].index(x) for x in id]
            except ValueError:
                raise ValueError("Couldn't identify the dimension 'id'")

        if items is not None and column is not None:
            if not isinstance(column, list):
                column = [column]
            try:
                index = [_block['column.id.x'].index(x) for x in column]
            except ValueError:
                raise ValueError("Couldn't identify the dimension 'column'")

        try:
            for i in index:
                for elem in items.keys():
                    _block['default'][i]['code_default'][elem] = items[elem]
                    _block['default'][i]['label_default'][elem] = None
        except ValueError:
            raise ValueError("Couldn't change the values")

    # adjust keys
    if type is "keys":
        id_xy = False
        method_xy = False
        class_xy = False

        # adjust items for missing elements
        if 'id.x' not in items.keys():
            items['id.x'] = None
        if 'id.y' not in items.keys():
            items['id.y'] = None
        if 'method' not in items.keys():
            items['method'] = None
        if 'class' not in items.keys():
            items['class'] = None

        if items['id.x'] is not None or items['id.y'] is not None:
            if not isinstance(items['id.x'], list):
                items['id.x'] = [items['id.x']]
            if not isinstance(items['id.y'], list):
                items['id.y'] = [items['id.y']]
            if len(items['id.x']) == len(items['id.x']):
                id_xy = True
            else:
                raise ValueError("Item ids have different lengths")

        if items['method'] is not None:
            if not isinstance(items['method'], list):
                items['method'] = [items['method']]
            if len(items['method']) == len(items['id.y']):
                method_xy = True
            else:
                raise ValueError("Methods have inconsistent lengths")

        if items['class'] is not None:
            if not isinstance(items['class'], list):
                items['class'] = [items['class']]
            if len(items['class']) == len(items['id.y']):
                class_xy = True
            else:
                raise ValueError("Classes have inconsistent lengths")

        try:
            if not isinstance(column, list):
                column = [column]
            for elem in _block:
                if not isinstance(elem['id.x'], list):
                    elem['id.x'] = [elem['id.x']]
                if not isinstance(elem['id.y'], list):
                    elem['id.y'] = [elem['id.y']]
                if elem['column.id.x'] in column:
                    if id_xy:  # change matching ids
                        for i in range(len(items['id.x'])):
                            if items['id.x'][i] in elem['id.x']:  # if id.x is already defined, change it
                                if items['id.y'][i] is None:  # if id.y is not defined, then remove it
                                    elem['id.y'].pop(
                                        elem['id.x'].index(items['id.x'][i]))
                                    elem['name.y'].pop(
                                        elem['id.x'].index(items['id.x'][i]))
                                    elem['method'].pop(
                                        elem['id.x'].index(items['id.x'][i]))
                                    elem['class'].pop(
                                        elem['id.x'].index(items['id.x'][i]))
                                    elem['name.x'].pop(
                                        elem['id.x'].index(items['id.x'][i]))
                                    elem['id.x'].pop(
                                        elem['id.x'].index(items['id.x'][i]))
                                else:  # if id.y is defined, then replace it
                                    elem['id.y'][elem['id.x'].index(
                                        items['id.x'][i])] = items['id.y'][i]
                                    elem['name.y'][elem['id.x'].index(items['id.x'][i])] = None
                                    if method_xy:
                                        elem['method'][elem['id.x'].index(
                                            items['id.x'][i])] = items['method'][i]
                                    else:
                                        elem['method'][elem['id.x'].index(
                                            items['id.x'][i])] = 'synonym-proximity-matching'
                                    if class_xy:
                                        elem['class'][elem['id.x'].index(
                                            items['id.x'][i])] = items['class'][i]
                                    else:
                                        elem['class'][elem['id.x'].index(
                                            items['id.x'][i])] = 'general'
                            else:  # if id.x is not already defined, add it
                                if items['id.y'][i] is not None:
                                    elem['id.x'].append(items['id.x'][i])
                                    elem['id.y'].append(items['id.y'][i])
                                    elem['name.x'].append(None)
                                    elem['name.y'].append(None)
                                    if method_xy:
                                        elem['method'].append(items['method'][i])
                                    else:
                                        elem['method'].append(
                                            'synonym-proximity-matching')
                                    if class_xy:
                                        elem['class'].append(items['class'][i])
                                    else:
                                        elem['class'].append('general')
        except ValueError:
            raise ValueError("Couldn't change the values")

    return(_block)


def fill(df=None, structure=None, keys=None, default=None,
         shape='vertical', inits=1, sep=',', learn=None,
         accu=0.75, form='full', token=None, email=None,
         url=None):
    """

    This command submits the request to the API fill endpoint
    to retrieve the desired extra data points. At the current stage
    dataset must be rectangular, and structure should be in the shape proposed
    analyze function. The minimum required by the endpoint is the data set and
    the corresponding structure. You can browse the available resources at
    https://rejustify.com/repos. Other features, including private
    resources and models, are taken as defined for the account.

    The API defines the submitted data set as x and any server-side data set as y.
    The corresponding structures are marked with the same principle, as structure.x and
    structure.y, for instance. The principle rule of any data manipulation is to never change
    data x (except for missing values), but only adjust y.

    Examples:
        # API setup
        setCurl()

        # register token/email
        register(token = "YOUR_TOKEN", email = "YOUR_EMAIL")

        # prepare sample data set
        df = pd.DataFrame()
        df['country'] = ['Italy'] * 4
        df['date'] = pd.date_range('2020-06-01', periods=4).strftime("%Y-%m-%d")
        df['covid cases'] = ""

        # rejustify
        st = analyze(df)
        rdf = fill(df, st)

    Attributes:
        df(DataFrame): The data set to be analyzed. Must be a DataFrame.
        structure(DataFrame): Structure of the x data set, characterizing classes, features, cleaners and formats
            of the columns/rows, and data provider/tables for empty columns. Perfectly, it should come from analyze
            endpoint.
        keys(list): The matching keys and matching methods between dimensions in x and y data sets. The elements in
            keys are determined based on information provided in data x and y, for each empty column. The details
            behind both data structures can be visualized by structure.x and structure.y.

            Matching keys are given consecutively, i.e. the first element in id.x and name.x corresponds
            to the first element in id.y and name.y, and so on. Dimension names are given for better readability of
            the results, however, they are not necessary for API recognition. keys return also data classification in
            element class and the proposed matching method for each part of id.x and id.y.

            Currently, API suports 6 matching methods: synonym-proximity-matching, synonym-matching, proximity-matching, time-matching,
            exact-matching and value-selection, which are given in a diminishing order of complexitiy. synonym-proximity-matching
            uses the proximity between the values in data x and y to the coresponding values in rejustify dictionary. If the proximity
            is above threshold accu and there are values in x and y pointing to the same element in the dictionary, the values will
            be matched. synonym-matching and proximity-matching use similar logic of either of the steps described for
            synonym-proximity-matching. time-matching aims at standardizing the time values to the same format before matching. For proper
            functioning it requires an accurate characterization of date format in structure.x (structure.y is already classified by rejustify).
            exact-matching will match two values only if they are identical. value-selection is a quasi matching method which for single-valued
            dimension x will return single value from y, as suggested by default specification. It is the most efficient
            matching method for dimensions which do not show any variability.
        default(dict): Default values used to lock dimensions in data y which will be not used for matching against
            data x. Each empty column to be filled, characterized by default['column.id.x'], must contain description of
            the default values. If missing, the API will propose the default values in line with the history of how it was
            used in the past.
        shape(str): It informs the API whether the data set should be read by
            columns (vertical) or by rows (horizontal). The current Python module support only vertical.
        inits(int): It informs the API how many initial rows (or columns in horizontal data), correspond
            to the header description. The default is inits=1.
        sep(str): The header can also be described by single field values, separated by a given character
            separator, for instance 'GDP, Austria, 1999'. The option informs the API which separator should
            be used to split the initial header string into corresponding dimensions. The default is sep=','.
        learn(bool): It should be set as True if the user accepts rejustify to track her/his activity
            to enhance the performance of the AI algorithms (it is not enabled by default). To change this option
            for all API calls run setCurl(learn = True).
        accu(float): Acceptable accuracy level on a scale from 0 to 1. It is used in the matching algorithms
            to determine string similarity. The default is accu=0.75.
        form(str): Requests the data to be returned either in full, or partial shape. The former returns the original
            data with filled empty columns. The latter returns only the filled columns.
        token(str): API token. By default read from global variables.
        email(str): E-mail address for the account. By default read from global variables.
        url(url): API url. By default read from global variables.
    """

    # error handling
    if df is not None and not isinstance(df, pd.DataFrame):
        raise ValueError("`df` parameter must be a DataFrame object")
    if df is None:
        raise ValueError("`df` parameter must be a DataFrame object")
    if structure is not None and not isinstance(structure, pd.DataFrame):
        raise ValueError("`structure` parameter must be a DataFrame object")
    if structure is None:
        raise ValueError("`structure` parameter must be a DataFrame object")
    if keys is not None and not isinstance(keys, list):
        raise ValueError("`keys` parameter must be a list object")
    if default is not None and not isinstance(default, dict):
        raise ValueError("`default` parameter must be a dict object")
    if shape is not "vertical":
        raise ValueError(
            "`shape` parameter must be vertical (horizontal tables are not yet supported in Python)")
    if inits is not None and not isinstance(inits, int):
        raise ValueError("`inits` parameter must be an integer")
    if inits is not None and inits > 1:
        raise ValueError("Currently `inits` can be max 1")
    if sep is not None and not isinstance(sep, str):
        raise ValueError("`sep` parameter must be a string")
    if len(sep) > 3:
        raise ValueError("`sep` has a maximum of 3 characters")
    if learn is not None and not isinstance(learn, bool):
        raise ValueError("`learn` parameter must be True/False")
    if accu is not None and not isinstance(accu, float):
        raise ValueError("`accu` parameter must be a float")
    if accu is not None and (accu > 1 or accu < 0):
        raise ValueError("`accu` parameter must be between 0 and 1")
    if form is not "full" and shape is not "reduced":
        raise ValueError("`form` parameter must be form/reduced")
    if token is not None and not isinstance(token, str):
        raise ValueError("`token` parameter must be a string")
    if email is not None and not isinstance(email, str):
        raise ValueError("`email` parameter must be a string")
    if url is not None and not isinstance(url, str):
        raise ValueError("`url` parameter must be a string")

    # set global variables
    if learn is None:
        learn = rejustify_learn
    if token is None:
        token = rejustify_token
    if email is None:
        email = rejustify_email
    if url is None:
        url = rejustify_main_url

    # reassign mutable objects
    _df = df.copy()
    _structure = structure.copy()
    _keys = None if keys is None else copy.deepcopy(keys)
    _default = None if default is None else copy.deepcopy(default)

    # convert DataFrame to array with correct naming
    _df.loc[-1] = _df.columns
    _df.index = _df.index + 1
    _df = _df.sort_index()

    # prepare the payload query
    payload = {}
    payload['structure'] = _structure.where(pd.notnull(_structure), None).to_dict('records')
    payload['data'] = _df.values.tolist()
    payload['keys'] = None if _keys is None else _keys
    if _default is not None:
        _dd = []
        for elem in _default['default']:
            _dd.append(elem.where(pd.notnull(elem), None).to_dict('records'))
        payload['meta'] = {'column.id.x': _default['column.id.x'],
                           'default': _dd}
    else:
        payload['meta'] = None
    payload['userToken'] = token
    payload['email'] = email
    payload['dataForm'] = form
    payload['dbAllowed'] = learn
    payload['minAccuracy'] = accu
    payload['sep'] = sep
    payload['direction'] = shape
    payload['inits'] = inits

    # send request
    if rejustify_proxy_url is not None and rejustify_proxy_port is not None:
        response = requests.post(rejustify_main_url + "/fill",  data=json.dumps(payload),
                                 headers={'Content-Type': 'application/json'},
                                 proxies={'http': rejustify_proxy_url + ':' + rejustify_proxy_port,
                                          'https': rejustify_proxy_url + ':' + rejustify_proxy_port})
    else:
        response = requests.post(rejustify_main_url + "/fill",  data=json.dumps(payload),
                                 headers={'Content-Type': 'application/json'})

    try:
        response_json = response.json()
    except ValueError:
        raise ValueError("Invalid response from rejustify (JSON expected)")

    if not response.ok:
        raise ValueError(response_json)

    # adjust column ids
    out_column = []
    for elem in response_json['structure']['out']['column']:
        out_column.append(elem[0])

    # adjust structure.y
    out_structure_y = []
    for elem in response_json['structure']['out']['meta']:
        out_structure_y.append(pd.DataFrame(elem))

    # adjust keys
    if response_json['structure']['out']['keys'] is not None:
        out_keys = []
        for elem in response_json['structure']['out']['keys']:
            if len(elem['id.x']) is 1:
                elem['id.x'] = elem['id.x'][0]
            if len(elem['name.x']) is 1:
                elem['name.x'] = elem['name.x'][0]
            if len(elem['id.y']) is 1:
                elem['id.y'] = elem['id.y'][0]
            if len(elem['name.y']) is 1:
                elem['name.y'] = elem['name.y'][0]
            if len(elem['class']) is 1:
                elem['class'] = elem['class'][0]
            if len(elem['method']) is 1:
                elem['method'] = elem['method'][0]
            if len(elem['column.id.x']) is 1:
                elem['column.id.x'] = elem['column.id.x'][0]
            if len(elem['column.name.x']) is 1:
                elem['column.name.x'] = elem['column.name.x'][0]
            out_keys.append(elem)

    # adjust default
    if response_json['structure']['out']['labels'] is not None:
        out_default = []
        for elem in response_json['structure']['out']['labels']:
            out_default.append(pd.DataFrame(elem).transpose())
            out_default[-1]['code_default'] = out_default[-1]['code_default'].str[0]
            out_default[-1]['label_default'] = out_default[-1]['label_default'].str[0]

    # output
    try:
        out = {'data': pd.DataFrame(response_json['structure']['out']['data']),
               'structure.x': pd.DataFrame(response_json['structure']['out']['structure']),
               'structure.y': {'column.id.x': out_column, 'structure.y': out_structure_y},
               'keys': out_keys,
               'default': {'column.id.x': out_column, 'default': out_default},
               'message': pd.DataFrame(response_json['structure']['message'])}
    except:
        out = "Consistency error. Check your input parameters."

    return(out)
