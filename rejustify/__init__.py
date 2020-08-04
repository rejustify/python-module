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
    setCurl()
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
    getCurl()
    """

    print('Main API address: ' + rejustify_main_url)
    print('Proxy address: ' + (rejustify_proxy_url if rejustify_proxy_url is not None else 'No proxy address'))
    print('Proxy port: ' + (rejustify_proxy_port if rejustify_proxy_port is not None else 'No proxy port'))
    print('Enable learning: ' + 'Yes' if rejustify_learn is True else 'No')


def register(token=None, email=None):
    """
    register()
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
    analyze()
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
    adjust()
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
    fill()
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
