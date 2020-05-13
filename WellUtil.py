import os
import fnmatch
import wellapplication as wa
import pandas as pd
import xmltodict
# Well Utility


def match_path(rootdir, pattern, remove_comp=False):
    """
    Returns a list of filepaths that match the fnmatch pattern within the given directory.
    :param path: filepath
    :param pattern: fnmatch pattern
    :return: list of paths
    """
    newlist = []
    DupList = list(set(newlist))
    for root, dirnames, filenames in os.walk(rootdir):
        for filename in fnmatch.filter(filenames, pattern):
            if os.path.join(root, filename) not in DupList:
                newlist.append(os.path.join(root, filename))
    print("Duplicates found: " + str(len(DupList)))

    if remove_comp == True:
        newlist = [k for k in newlist if 'Compensated' not in k]

    return newlist

def solinst_df(path_list):
    """
    Create list of DataFrames from list of filepaths.
    :param path_list:
    :return: 
    """
    dfs = []
    for counter, f in enumerate(path_list):
        if f.endswith('.xle'):
            dfs.append(new_xle_imp(f))
            print(counter)
        elif f.endswith('.lev'):
            dfs.append(new_lev_imp(f))
            print(counter)
    return dfs


def printmes(x):
    try:
        from arcpy import AddMessage
        AddMessage(x)
        print(x)
    except ModuleNotFoundError:
        print(x)


def new_xle_imp(infile):
    """This function uses an exact file path to upload a xle transducer file.
    Args:
        infile (file):
            complete file path to input file
    Returns:
        A Pandas DataFrame containing the transducer data
    """
    # open text file
    with open(infile, "rb") as f:
        obj = xmltodict.parse(f, xml_attribs=True, encoding="ISO-8859-1")
    # navigate through xml to the data
    wellrawdata = obj['Body_xle']['Data']['Log']
    # convert xml data to pandas dataframe
    try:
        f = pd.DataFrame(wellrawdata)
    except ValueError:
        printmes('xle file {:} incomplete'.format(infile))
        return
    # CH 3 check
    try:
        ch3ID = obj['Body_xle']['Ch3_data_header']['Identification']
        f[str(ch3ID).title()] = f['ch3']
    except(KeyError, UnboundLocalError):
        pass

    # CH 2 manipulation
    try:
        ch2ID = obj['Body_xle']['Ch2_data_header']['Identification']
        f[str(ch2ID).title()] = f['ch2']
        ch2Unit = obj['Body_xle']['Ch2_data_header']['Unit']
        numCh2 = pd.to_numeric(f['ch2'])
        if ch2Unit == 'Deg C' or ch2Unit == u'\N{DEGREE SIGN}' + u'C':
            f[str(ch2ID).title()] = numCh2
        elif ch2Unit == 'Deg F' or ch2Unit == u'\N{DEGREE SIGN}' + u'F':
            printmes('Temp in F, converting to C')
            f[str(ch2ID).title()] = (numCh2 - 32) * 5 / 9
        f[str(ch2ID).title()] = pd.to_numeric(f[str(ch2ID).title()])
    except (KeyError,UnboundLocalError):
        printmes('No channel 2 for {:}'.format(infile))
    # CH 1 manipulation
    ch1ID = obj['Body_xle']['Ch1_data_header']['Identification']  # Usually level
    ch1Unit = obj['Body_xle']['Ch1_data_header']['Unit']  # Usually ft
    unit = str(ch1Unit).lower()

    if unit == "feet" or unit == "ft":
        f[str(ch1ID).title()] = pd.to_numeric(f['ch1'])
    elif unit == "kpa":
        f[str(ch1ID).title()] = pd.to_numeric(f['ch1']) * 0.33456
        printmes("Units in kpa, converting {:} to ft...".format(os.path.basename(infile)))
    elif unit == "mbar":
        f[str(ch1ID).title()] = pd.to_numeric(f['ch1']) * 0.0334552565551
    elif unit == "psi":
        f[str(ch1ID).title()] = pd.to_numeric(f['ch1']) * 2.306726
        printmes("Units in psi, converting {:} to ft...".format(os.path.basename(infile)))
    elif unit == "m" or unit == "meters":
        f[str(ch1ID).title()] = pd.to_numeric(f['ch1']) * 3.28084
        printmes("Units in psi, converting {:} to ft...".format(os.path.basename(infile)))
    else:
        f[str(ch1ID).title()] = pd.to_numeric(f['ch1'])
        printmes("Unknown units, no conversion")

    # add extension-free file name to dataframe
    f['name'] = infile.split('\\').pop().split('/').pop().rsplit('.', 1)[0]
    # combine Date and Time fields into one field
    f['DateTime'] = pd.to_datetime(f.apply(lambda x: x['Date'] + ' ' + x['Time'], 1))
    f[str(ch1ID).title()] = pd.to_numeric(f[str(ch1ID).title()])

    # add logger information to dataframe
    model = obj['Body_xle']['Instrument_info']['Instrument_type']
    f['model'] = model
    serial = obj['Body_xle']['Instrument_info']['Serial_number']
    f['sn'] = serial
    site_loc = obj['Body_xle']['Instrument_info_data_header']['Location']
    f['location'] = site_loc

    try:
        ch3ID = obj['Body_xle']['Ch3_data_header']['Identification']
        f[str(ch3ID).title()] = pd.to_numeric(f[str(ch3ID).title()])
    except(KeyError, UnboundLocalError):
        pass

    f = f.reset_index()
    f = f.set_index('DateTime')
    f['Level'] = f[str(ch1ID).title()]
    f = f.drop(['Date', 'Time', '@id', 'ch1', 'ch2', 'index', 'ms'], axis=1)

    return f


def new_lev_imp(infile):
    with open(infile, "r") as fd:
        txt = fd.readlines()

    try:
        data_ind = txt.index('[Data]\n')
        # inst_info_ind = txt.index('[Instrument info from data header]\n')
        ch1_ind = txt.index('[CHANNEL 1 from data header]\n')
        ch2_ind = txt.index('[CHANNEL 2 from data header]\n')
        level = txt[ch1_ind + 1].split('=')[-1].strip().title()
        level_units = txt[ch1_ind + 2].split('=')[-1].strip().lower()
        temp = txt[ch2_ind + 1].split('=')[-1].strip().title()
        temp_units = txt[ch2_ind + 2].split('=')[-1].strip().lower()
        # serial_num = txt[inst_info_ind+1].split('=')[-1].strip().strip(".")
        # inst_num = txt[inst_info_ind+2].split('=')[-1].strip()
        # location = txt[inst_info_ind+3].split('=')[-1].strip()
        # start_time = txt[inst_info_ind+6].split('=')[-1].strip()
        # stop_time = txt[inst_info_ind+7].split('=')[-1].strip()

        df = pd.read_table(infile, parse_dates=[[0, 1]], sep='\s+', skiprows=data_ind + 2,
                           names=['Date', 'Time', level, temp],
                           skipfooter=1, engine='python')
        df.rename(columns={'Date_Time': 'DateTime'}, inplace=True)
        df.set_index('DateTime', inplace=True)

        if level_units == "feet" or level_units == "ft":
            df[level] = pd.to_numeric(df[level])
        elif level_units == "kpa":
            df[level] = pd.to_numeric(df[level]) * 0.33456
            printmes("Units in kpa, converting {:} to ft...".format(os.path.basename(infile)))
        elif level_units == "mbar":
            df[level] = pd.to_numeric(df[level]) * 0.0334552565551
        elif level_units == "psi":
            df[level] = pd.to_numeric(df[level]) * 2.306726
            printmes("Units in psi, converting {:} to ft...".format(os.path.basename(infile)))
        elif level_units == "m" or level_units == "meters":
            df[level] = pd.to_numeric(df[level]) * 3.28084
            printmes("Units in psi, converting {:} to ft...".format(os.path.basename(infile)))
        else:
            df[level] = pd.to_numeric(df[level])
            printmes("Unknown units, no conversion")

        if temp_units == 'Deg C' or temp_units == u'\N{DEGREE SIGN}' + u'C':
            df[temp] = df[temp]
        elif temp_units == 'Deg F' or temp_units == u'\N{DEGREE SIGN}' + u'F':
            printmes('Temp in F, converting {:} to C...'.format(os.path.basename(infile)))
            df[temp] = (df[temp] - 32.0) * 5.0 / 9.0
        df['name'] = infile
        return df
    except ValueError:
        printmes('File {:} has formatting issues'.format(infile))
