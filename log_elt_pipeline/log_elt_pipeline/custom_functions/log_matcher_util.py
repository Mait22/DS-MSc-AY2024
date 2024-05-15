import numpy as np
import pandas as pd
from functools import reduce
import datetime


def pattern_comp(value: np.array,
                 test_pattern: np.array,
                 permutation_subsets: list[list] = [],
                 wildcard="*") -> bool:
    """

    :param value: atomic event sequence to compare with test_pattern
    :param test_pattern: test_pattern
    :param permutation_subsets: subsets of test pattern which allow for matching across possible permutations
    :param wildcard: wildcard signifier, which mathces with all atomic events at given position

    :return: boolean indicating if match was found
    """

    assert len(test_pattern) == len(value)

    # Create copy of inputs
    c_value = np.copy(value)
    c_test_pattern = np.copy(test_pattern)

    # Create positional wildcards in value array
    c_value[c_test_pattern == wildcard] = wildcard

    # Order by subset to deal with event order permutations
    if len(permutation_subsets) > 0:
        for per in permutation_subsets:
            c_value[per[0]:per[1]] = np.sort(c_value[per[0]:per[1]])
            c_test_pattern[per[0]:per[1]] = np.sort(c_test_pattern[per[0]:per[1]])

    ## Make comparison
    return np.sum(c_value == c_test_pattern) == len(value)

def filter_and_prep_log_df(lc_name: str,
                           signature_col: str,
                           log_data: pd.DataFrame,
                           pattern_arrays: list[dict],

                           # Input df column names
                           lc_col_name,
                           start_datetime_col: str,
                           end_datetime_col: str,

                           # Output df column names
                           matched_event_id_col: str,
                           matched_event_pattern_col: str,
                           matched_event_duration_col: str,
                           log_event_duration_col: str,
                           matched_event_type_col: str,
                           log_event_time_diff_col: str,
                           ) -> pd.DataFrame:
    """

    atomic evens - source events which compose train passing event signature, i.e. the algorithm
                   matches atomic event to pre-specified patterns

    :param lc_name: level crossing name
    :param signature_col: column of input data (parameter log_data) that contains atomic log event type
    :param log_data: input data of atomic events
    :param pattern_arrays: list of pattern array
    :param lc_col_name: column of input data that contains level crossing names
    :param start_datetime_col: atomic log event start datetime column
    :param end_datetime_col: atomic log event end datetime column
    :param matched_event_id_col: output dataframe column name for matched event id (generated automatically)
    :param matched_event_pattern_col: output dataframe column name for matched event signature (generated automatically
                                                                                                based on pattern arrays)
    :param matched_event_duration_col: output dataframe column name for matched event duration (generated automatically)
    :param log_event_duration_col: atomic event
    :param matched_event_type_col: output dataframe column name for matched event signature (generated automatically
                                                                                                based on pattern arrays)
    :param log_event_time_diff_col: diff between sequential atomic events

    :return: data frame prepared for event matching
    """

    # Keep only log events patterns applicable to specified lever crossing
    pattern_arrays_lc_specific = list(filter(lambda el: lc_name in el["aplicable_to"], pattern_arrays))
    pattern_arrays_lc_specific = [el["pattern"] for el in pattern_arrays_lc_specific]
    pattern_arrays_lc_specific = list(reduce(lambda l1, l2: l1 + l2, pattern_arrays_lc_specific, []))
    pattern_arrays_lc_specific = list(set(pattern_arrays_lc_specific))

    # Keep only lever crossing specific log events
    log_data = log_data[log_data[signature_col].isin(pattern_arrays_lc_specific)]
    log_data = log_data[log_data[lc_col_name] == lc_name].sort_values(by=[start_datetime_col])

    # Compute time diff and event durations
    pd.options.mode.copy_on_write = True

    log_data[log_event_time_diff_col] = pd.to_datetime(log_data[start_datetime_col]).diff()
    log_data[log_event_duration_col] = pd.to_datetime(log_data[end_datetime_col]) - pd.to_datetime(
        log_data[start_datetime_col])

    # Add placeholder columns for matched event id, matched event pattern type and matched event duration
    log_data[matched_event_id_col] = np.nan
    log_data[matched_event_duration_col] = np.nan
    log_data[matched_event_type_col] = np.nan
    log_data[matched_event_pattern_col] = np.nan

    # Reset index for pattern match iteration
    log_data = log_data.reset_index()

    return log_data


def log_pattern_matcher(log_data: pd.DataFrame,
                        signature_col: str,
                        log_data_id: str,
                        lc_name: str,
                        time_diff_criteria: float = 0.9,
                        pattern_arrays: list[dict] = None,
                        subevent_pattern_arrays: list[dict] = None,

                        # Input DF
                        start_datetime_col: str = "MainLogEventStartTime",
                        end_datetime_col: str = "MainLogEventEndTime",
                        lc_col_name: str = "LcName",

                        # Output DF
                        matched_event_id_col: str = "MatchedEventId",
                        matched_event_pattern_col: str = "MatchedEventSignature",
                        matched_event_duration_col: str = "MatchedEventDuration",
                        log_event_duration_col: str = "LogEventDuration",
                        matched_event_type_col: str = "EventType",
                        log_event_time_diff_col: str = "TimeDiff"

                        ) -> {pd.DataFrame}:
    """

    :param log_data: source dataframe
    :param signature_col: log event str type from input data
    :param log_data_id: source atomic log event's id col
    :param lc_name: level crossing name 
    :param time_diff_criteria: if starting or ending two events compose more than this amount of time form whole matched event's duration, discard given matching
    :param pattern_arrays: patterns to match
    :param start_datetime_col: source log event start 
    :param end_datetime_col: source log event end
    :param lc_col_name: source data's level crossing col name
    :param matched_event_id_col: return data's col for matched event id (concatenates source log entry's ID-s)
    :param matched_event_pattern_col: return data's col for matched event signature (concatenates source log entry's signature_col values)
    :param matched_event_duration_col: return data's col for matched event duration
    :param log_event_duration_col: source log event duration col
    :param matched_event_type_col: return data's col to specify what kind of event signature template was matched
    :param log_event_time_diff_col: sequential time diff bewteen atomic source log entries 

    :return:
    """

    if pattern_arrays is None:
        pattern_arrays = [

            ## Moe
            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemine B suunal",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 1",
             "aplicable_to": ["Moe", "Mullavere", "Mõneku", "Mägiste", "Poldri", "Sompa", "Sootaga", "Sordi"],
             "permutations": [[0, 4], [4, 6]]
             },

            {"pattern": ["Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 2",
             "aplicable_to": ["Moe", "Mullavere", "Mõneku", "Mägiste", "Poldri", "Sompa", "Sootaga", "Sordi"],
             "permutations": [[0, 4], [4, 6]]
             },

            ## Aiamaa, Tamsalu, Kärkna, Kabala, Kadrina, Kalme, Kiviõli
            {"pattern": ["ETS aktiveerimisliidese käivitus",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine"],
             "event_type": "Rongi läbimise muster 3",
             "aplicable_to": ["Aiamaa", "Tamsalu", "Kärkna", "Kabala", "Kadrina", "Kalme", "Keeni", "Kiviõli",
                              "Lemmatsi", "Nõo", "Näki", "Oru",
                              "Palupera", "Peedu", "Puka", "Püssi", "Ropka", "Soldina", "Sangaste", "Sonda", "Tambre"],
             "permutations": [[0, 3]]},

            ## Kulli

            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Lähenemine A suunal",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 4 - šundi kaotus?",
             "aplicable_to": ["Kulli", "Kuru", "Lehtse", "Mustjõe", "Parila", "Tiksoja"],
             "permutations": [[0, 5], [5, 7], [7, 9], [9, 11]],
             },

            {"pattern": ["Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Lähenemine B suunal",

                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",

                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus",

                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 5",
             "aplicable_to": ["Tiksoja"],
             "permutations": [[0, 6], [6, 8], [8, 10]]
             },

            {"pattern": ["Lähenemine B suunal",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna B1 hõivatus",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna BC hõivatus",
                         "Lähenemispiirkonna B2 hõivatus",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 6",
             "aplicable_to": ["Kulli", "Kuru", "Lehtse", "Mustjõe", "Parila", "Tiksoja"],
             "permutations": [[0, 6], [6, 8], [8, 10]]
             },

            {"pattern": ["Lähenemine A suunal",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 7",
             "aplicable_to": ["Kulli", "Kuru", "Lehtse", "Mustjõe", "Parila", "Tiksoja"],
             "permutations": [[0, 6], [6, 8], [8, 10]]
             },

            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemispiirkonna B1 hõivatus",
                         "Lähenemine B suunal",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna BC hõivatus",
                         "Lähenemispiirkonna B2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 8",
             "aplicable_to": ["Kulli", "Kuru", "Lehtse", "Mustjõe", "Parila", "Tiksoja"],
             "permutations": [[0, 5], [5, 7], [7, 9]]
             },

            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Lähenemine A suunal",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 9",
             "aplicable_to": ["Kulli", "Kuru", "Lehtse", "Mustjõe", "Parila", "Tiksoja"],
             "permutations": [[0, 3], [3, 5], [5, 7]]
             },

            ## Lagedi
            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Lähenemine A suunal",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 11",
             "aplicable_to": ["Lagedi"],
             "permutations": [[0, 6], [6, 8], [8, 10]]
             },

            {"pattern": ["Lähenemispiirkonna A2 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 12",
             "aplicable_to": ["Lagedi"],
             "permutations": [[0, 5], [5, 7], [7, 9]]
             },

            {"pattern": ["Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 13",
             "aplicable_to": ["Lagedi"],
             "permutations": [[0, 4], [4, 6], [6, 8]]
             },

            {"pattern": ["Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine",
                         ],
             "event_type": "Rongi läbimise muster 14",
             "aplicable_to": ["Lagedi", "Tabivere"],
             "permutations": [[0, 2], [2, 4], [4, 6]]
             },

            ## Kohtla
            {"pattern": ["Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 15",
             "aplicable_to": ["Kohtla"],
             "permutations": [[0, 4], [4, 6]]
             },

            {"pattern": ["Fooride vilkumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 16",
             "aplicable_to": ["Kohtla"],
             "permutations": [[0, 3], [3, 5]]
             },

            ## Kiidjärve
            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 17",
             "aplicable_to": ["Kiidjärve", "Ruusa", "Taevaskoja"],
             "permutations": [[0, 3], [3, 5]]
             },

            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 18",
             "aplicable_to": ["Kiidjärve", "Ruusa", "Taevaskoja"],
             "permutations": [[0, 3], [3, 5]]
             },

            ## Kesk-kaar
            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 19",
             "aplicable_to": ["Kesk-kaar"],
             "permutations": [[0, 3], [3, 5], [5, 7]]
             },

            {"pattern": ["Lähenemispiirkonna A2 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 20",
             "aplicable_to": ["Kesk-kaar"],
             "permutations": [[0, 3], [3, 5], [5, 7]]
             },

            ## Kehra
            {"pattern": ["Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemine B suunal",
                         "Lähenemispiirkonna B1 hõivatus",
                         "Lähenemispiirkonna BC hõivatus",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine",
                         ],
             "event_type": "Rongi läbimise muster 21",
             "aplicable_to": ["Kehra"],
             "permutations": [[0, 4], [4, 6], [6, 11], [11, 13]]
             },

            {"pattern": ["Lähenemispiirkonna B1 hõivatus",
                         "Lähenemine B suunal",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Lähenemine A suunal",
                         "Lähenemispiirkonna BC hõivatus",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 22",
             "aplicable_to": ["Kehra"],
             "permutations": [[0, 4], [4, 6], [6, 11], [11, 13]]
             },

            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Lähenemine A suunal",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine",
                         ],
             "event_type": "Rongi läbimise muster 23",
             "aplicable_to": ["Kehra"],
             "permutations": [[0, 6], [6, 8], [8, 10]]
             },

            {"pattern": ["Lähenemispiirkonna B1 hõivatus",
                         "Lähenemine B suunal",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna BC hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 24",
             "aplicable_to": ["Kehra"],
             "permutations": [[0, 4], [4, 6], [6, 9]]
             },

            ## Kalevi
            {"pattern": ["Lähenemispiirkonna A2 hõivatus",
                         "Lähenemine B suunal",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus",
                         ],
             "event_type": "Rongi läbimise muster 25",
             "aplicable_to": ["Kalevi"],
             "permutations": [[0, 4], [4, 6]]
             },

            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 26",
             "aplicable_to": ["Kalevi"],
             "permutations": [[0, 3], [3, 5]]
             },

            ## Jäneda
            {"pattern": ["Lähenemispiirkonna B1 hõivatus",
                         "Lähenemine B suunal",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna BC hõivatus",
                         "Lähenemispiirkonna B2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 27",
             "aplicable_to": ["Jäneda"],
             "permutations": [[0, 4], [4, 6], [6, 8], [8, 10]]
             },

            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Lähenemine A suunal",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 28",
             "aplicable_to": ["Jäneda"],
             "permutations": [[0, 4], [4, 6], [6, 8], [8, 10]]
             },

            ## Irvala
            {"pattern": ["Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Lähenemine A suunal",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 29",
             "aplicable_to": ["Irvala"],
             "permutations": [[0, 4], [4, 6]]
             },

            {"pattern": ["Lähenemispiirkonna A2 hõivatus",
                         "Lähenemine B suunal",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 30",
             "aplicable_to": ["Irvala"],
             "permutations": [[0, 4], [4, 6]]
             },

            ## Aardla tn
            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine"
                         ],
             "event_type": "Rongi läbimise muster 31",
             "aplicable_to": ["Aardla tn"],
             "permutations": []
             },

            {"pattern": ["Lähenemispiirkonna A2 hõivatus",
                         "Ülesõidu rakendumine"
                         ],
             "event_type": "Rongi läbimise muster 32",
             "aplicable_to": ["Aardla tn"],
             "permutations": []
             },

            ## Aardla
            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 33",
             "aplicable_to": ["Aardla"],
             "permutations": [[0, 3], [3, 5], [5, 8]]
             },

            {"pattern": ["Lähenemispiirkonna A2 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 34",
             "aplicable_to": ["Aardla"],
             "permutations": [[0, 3], [3, 5], [5, 8]]
             },

            ## Rakvere
            {"pattern": ["ETS aktiveerimisliidese käivitus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 35",
             "aplicable_to": ["Rakvere", "Põlva", "Tapa"],
             "permutations": [[0, 3], [3, 5], [5, 7]]
             },

            {"pattern": ["Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 36",
             "aplicable_to": ["Rakvere", "Põlva", "Tapa"],
             "permutations": [[0, 2], [2, 4], [4, 6]]
             },

            ## Raasiku
            {"pattern": ["ETS aktiveerimisliidese käivitus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 37",
             "aplicable_to": ["Raasiku"],
             "permutations": [[0, 3], [3, 5], [5, 7]]
             },

            ## Vägeva +
            {"pattern": ["Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",

                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",

                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 38",
             "aplicable_to": ["Vägeva"],
             "permutations": [[0, 6], [6, 8], [8, 10]]
             },

            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Fooride vilkumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 39",
             "aplicable_to": ["Vägeva"],
             "permutations": [[0, 5], [5, 7], [7, 9]]
             },

            ## Aruküla ja Betooni +
            {"pattern": ["Fooride vilkumine",
                         "Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 40",
             "aplicable_to": ["Aruküla"],
             "permutations": [[0, 7], [7, 9], [9, 11]]
             },

            {"pattern": ["Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",

                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",

                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 41",
             "aplicable_to": ["Aruküla", "Betooni"],
             "permutations": [[0, 6], [6, 8], [8, 10]]
             },

            {"pattern": ["Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 42",
             "aplicable_to": ["Aruküla"],
             "permutations": [[0, 6], [6, 8]]
             },

            {"pattern": ["Ülesõidu rakendumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 43",
             "aplicable_to": ["Aruküla"],
             "permutations": [[0, 6], [6, 8], [8, 10]]},

            {"pattern": ["Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 44",
             "aplicable_to": ["Aruküla"],
             "permutations": [[0, 4], [4, 6]]},

            {"pattern": ["Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B tõstmine",
                         "Tõkkepuu A tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 45",
             "aplicable_to": ["Aruküla"],
             "permutations": [[0, 1], [1, 3], [3, 5]]},

            ## Holvandi +
            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 46",
             "aplicable_to": ["Holvandi"],
             "permutations": [[0, 3], [3, 5]]},

            {"pattern": ["Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus",
                         ],
             "event_type": "Rongi läbimise muster 47",
             "aplicable_to": ["Holvandi"],
             "permutations": [[0, 2], [2, 5]]},

            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 48",
             "aplicable_to": ["Holvandi"],
             "permutations": [[0, 3], [3, 5]]},

            {"pattern": ["Lähenemispiirkonna A2 hõivatus",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine"
                         ],
             "event_type": "Rongi läbimise muster 49",
             "aplicable_to": ["Holvandi"],
             "permutations": [[0, 4]]},

            {"pattern": ["Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Lähenemispiirkonna AC hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 50",
             "aplicable_to": ["Holvandi"],
             "permutations": [[0, 4]]},

            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Fooride vilkumine"
                         ],
             "event_type": "Rongi läbimise muster 51",
             "aplicable_to": ["Holvandi"],
             "permutations": [[0, 3]]},

            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Fooride vilkumine"
                         ],
             "event_type": "Rongi läbimise muster 52",
             "aplicable_to": ["Holvandi"],
             "permutations": [[0, 3]]},

            ## Betooni +
            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Fooride vilkumine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",

                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus",

                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 53",
             "aplicable_to": ["Betooni"],
             "permutations": [[0, 5], [5, 7], [7, 9]]},

            {"pattern": ["Lähenemispiirkonna A1 hõivatus",
                         "Lähenemine A suunal",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",

                         "Tõkkepuu A langetamine",
                         "Tõkkepuu B langetamine",

                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 54",
             "aplicable_to": ["Betooni"],
             "permutations": [[0, 4], [4, 6], [6, 8]]},

            ## Auvere +
            {"pattern": ["Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "ETS aktiveerimisliidese käivitus"
                         ],
             "event_type": "Rongi läbimise muster 55",
             "aplicable_to": ["Auvere", "Elva"],
             "permutations": [[0, 3]]},

            {"pattern": ["Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         ],
             "event_type": "Rongi läbimise muster 56",
             "aplicable_to": ["Auvere", "Elva"],
             "permutations": [[0, 2]]},

            ## Ilumetsa +
            {"pattern": ["Lähenemine A suunal",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 57",
             "aplicable_to": ["Ilumetsa"],
             "permutations": [[0, 3], [3, 6]]},

            {"pattern": ["Lähenemine A suunal",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A1 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 58",
             "aplicable_to": ["Ilumetsa"],
             "permutations": [[0, 6]]},

            {"pattern": ["Fooride vilkumine",
                         "Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine"
                         ],
             "event_type": "Rongi läbimise muster 59",
             "aplicable_to": ["Ilumetsa"],
             "permutations": [[0, 4]]},

            ## Imastu -
            {"pattern": ["Ülesõidu rakendumine",
                         "Lähenemine A suunal",
                         "Lähenemispiirkonna A1 hõivatus",
                         "Ülesõidu rakendumine",
                         "Fooride vilkumine",
                         "Lähenemispiirkonna AC hõivatus",
                         "Lähenemispiirkonna A2 hõivatus",
                         "Lähenemispiirkonna A1 hõivatus"
                         ],
             "event_type": "Rongi läbimise muster 60",
             "aplicable_to": ["Imastu"],
             "permutations": [[0, 5], [5, 8]]},

            ## Aegviidu, Ülejõe +
            {"pattern": ["Fooride vilkumine",
                         "Ülesõidu rakendumine",
                         "ETS aktiveerimisliidese käivitus",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine"
                         ],
             "event_type": "Rongi läbimise muster 61",
             "aplicable_to": ["Aegviidu", "Ülejõe"],
             "permutations": [[0, 3], [3, 7]]},

            {"pattern": ["ETS aktiveerimisliidese käivitus",
                         "Ülesõidu rakendumine",
                         "Tõkkepuu B langetamine",
                         "Tõkkepuu A langetamine",
                         "Tõkkepuu A tõstmine",
                         "Tõkkepuu B tõstmine",
                         ],
             "event_type": "Rongi läbimise muster 62",
             "aplicable_to": ["Aegviidu", "Ülejõe"],
             "permutations": [[0, 2], [2, 6]]},

            ## Aiamaa, Tamsalu, Kärkna, Kabala, Kadrina, Kalme, Kiviõli
            {"pattern": ["ETS aktiveerimisliidese käivitus",
                         "Fooride vilkumine",
                         "Ülesõidu rakendumine"],
             "event_type": "Rongi läbimise muster 63",
             "aplicable_to": ["Aiamaa", "Tamsalu", "Kärkna", "Kabala", "Kadrina", "Kalme", "Keeni", "Kiviõli",
                              "Lemmatsi", "Nõo", "Näki", "Oru",
                              "Palupera", "Peedu", "Puka", "Püssi", "Ropka", "Soldina", "Sangaste", "Sonda", "Tambre"],
             "permutations": [[0, 3]]},

            ## Orava
            {"pattern": ["ETS aktiveerimisliidese käivitus",
                         "Ülesõidu rakendumine"],
             "event_type": "Rongi läbimise muster 64",
             "aplicable_to": ["Orava"],
             "permutations": [[0, 2]]},
        ]

    pattern_arrays = list(filter(lambda el: lc_name in el["aplicable_to"], pattern_arrays))

    log_data_input = log_data[log_data[lc_col_name] == lc_name]

    ## Prepare DF
    log_data = filter_and_prep_log_df(lc_name,
                                      signature_col,
                                      log_data,
                                      pattern_arrays,

                                      lc_col_name,
                                      start_datetime_col,
                                      end_datetime_col,

                                      matched_event_id_col,
                                      matched_event_pattern_col,
                                      matched_event_duration_col,
                                      log_event_duration_col,
                                      matched_event_type_col,
                                      log_event_time_diff_col)

    ## Matching events
    for pt_dict in pattern_arrays:
        l = len(pt_dict["pattern"])
        pt_str = "_".join(pt_dict["pattern"])
        pt = np.asarray(pt_dict["pattern"])

        r = 0
        while r < len(log_data):
            if l + r > len(log_data):
                break
            elif log_data.loc[r:(r + l - 1), matched_event_id_col].isnull().all():
                if pattern_comp(np.asarray(log_data.loc[r:(r + l - 1), signature_col]), pt, pt_dict["permutations"]):

                    duration = (pd.to_datetime(log_data.loc[(r + l - 1), end_datetime_col]) - pd.to_datetime(
                        log_data.loc[r, start_datetime_col]))

                    # Discard events which first or last atomic constituent event
                    # encompass overly large proportion of matched event cascade duration
                    # specified by parameter time_diff_criteria
                    if ((log_data.loc[(r + l - 1), log_event_time_diff_col] / duration <= time_diff_criteria) and
                            (log_data.loc[(r + 1), log_event_time_diff_col] / duration <= time_diff_criteria)
                            #or duration < datetime.timedelta(seconds=5) <<<< what is that?
                            ):

                        log_data.loc[r:(r + l - 1), matched_event_pattern_col] = pt_str
                        log_data.loc[r:(r + l - 1), matched_event_type_col] = pt_dict["event_type"]
                        log_data.loc[r:(r + l - 1), matched_event_id_col] = "_".join(
                            [str(i) for i in list(log_data.loc[r:(r + l - 1), log_data_id])])
                        log_data.loc[r:(r + l - 1), matched_event_duration_col] = duration

                    else:
                        print(r)
            r += 1

    return {'filtered_events': log_data[["LcName",
                                         "MainLogEventStartTime",
                                         "MainLogEventId",
                                         "TimeDiff",
                                         "LogEventDuration",
                                         "MatchedEventId",
                                         "MatchedEventDuration",
                                         "MatchedEventSignature"]],
            'all_events': log_data_input.merge(log_data[["MainLogEventId",
                                                         "TimeDiff",
                                                         "LogEventDuration",
                                                         "MatchedEventId",
                                                         "MatchedEventDuration",
                                                         "MatchedEventSignature"]],
                                               left_on='MainLogEventId',
                                               right_on='MainLogEventId',
                                               suffixes=('_all', '_matched'),
                                               how='left')}


def widen_matched_event(filtered_events: pd.DataFrame,
                        all_events: pd.DataFrame,
                        matched_event_id_col: str,
                        matched_event_id: str,
                        main_event_id_col: str,
                        main_log_event_col: str,
                        start_datetime_col: str,
                        end_datetime_col: str):
    assert len(filtered_events) > 1

    matched_event = filtered_events[filtered_events[matched_event_id_col] == matched_event_id].sort_values(
        by=[start_datetime_col]).reset_index(drop=True)
    start = matched_event.loc[0, [start_datetime_col]]
    end = matched_event.loc[len(matched_event) - 1, [end_datetime_col]]
    print(start, end)

    return all_events[(all_events[start_datetime_col] >= start[start_datetime_col]) & (
                all_events[end_datetime_col] <= end[end_datetime_col])].sort_values(by=[start_datetime_col])


