proper_routes = [
    'IV',
    'PO',
    'PO/NG',
    'ORAL',
    'IV DRIP',
    'IV BOLUS',
]

proper_drug_types = [
    'MAIN',
    'ADDITIVE',
]

abxs = [
    'adoxa',
    'ala-tet',
    'alodox',
    'amikacin',
    'amikin',
    'amoxicillin',
    # 'amoxicillin%claulanate',
    'clavulanate',
    'ampicillin',
    'augmentin',
    'avelox',
    'avidoxy',
    'azactam',
    'azithromycin',
    'aztreonam',
    'axetil',
    'bactocill',
    'bactrim',
    'bethkis',
    'biaxin',
    'bicillin l-a',
    'cayston',
    'cefazolin',
    'cedax',
    'cefoxitin',
    'ceftazidime',
    'cefaclor',
    'cefadroxil',
    'cefdinir',
    'cefditoren',
    'cefepime',
    'cefotetan',
    'cefotaxime',
    'cefpodoxime',
    'cefprozil',
    'ceftibuten',
    'ceftin',
    'cefuroxime',
    'cefuroxime',
    'cephalexin',
    'chloramphenicol',
    'cipro',
    'ciprofloxacin',
    'claforan',
    'clarithromycin',
    'cleocin',
    'clindamycin',
    'cubicin',
    'dicloxacillin',
    'doryx',
    'doxycycline',
    'duricef',
    'dynacin',
    'ery-tab',
    'eryped',
    'eryc',
    'erythrocin',
    'erythromycin',
    'factive',
    'flagyl',
    'fortaz',
    'furadantin',
    'garamycin',
    'gentamicin',
    'kanamycin',
    'keflex',
    'ketek',
    'levaquin',
    'levofloxacin',
    'lincocin',
    'macrobid',
    'macrodantin',
    'maxipime',
    'mefoxin',
    'metronidazole',
    'minocin',
    'minocycline',
    'monodox',
    'monurol',
    'morgidox',
    'moxatag',
    'moxifloxacin',
    'myrac',
    'nafcillin sodium',
    'nicazel doxy 30',
    'nitrofurantoin',
    'noroxin',
    'ocudox',
    'ofloxacin',
    'omnicef',
    'oracea',
    'oraxyl',
    'oxacillin',
    'pc pen vk',
    'pce dispertab',
    'panixine',
    'pediazole',
    'penicillin',
    'periostat',
    'pfizerpen',
    'piperacillin',
    'tazobactam',
    'primsol',
    'proquin',
    'raniclor',
    'rifadin',
    'rifampin',
    'rocephin',
    'smz-tmp',
    'septra',
    'septra ds',
    'septra',
    'solodyn',
    'spectracef',
    'streptomycin sulfate',
    'sulfadiazine',
    'sulfamethoxazole',
    'trimethoprim',
    'sulfatrim',
    'sulfisoxazole',
    'suprax',
    'synercid',
    'tazicef',
    'tetracycline',
    'timentin',
    'tobi',
    'tobramycin',
    'trimethoprim',
    'unasyn',
    'vancocin',
    'vancomycin',
    'vantin',
    'vibativ',
    'vibra-tabs',
    'vibramycin',
    'zinacef',
    'zithromax',
    'zmax',
    'zosyn',
    'zyvox'
]

suspected_infection_terms = """
                  , CASE WHEN charttime IS NOT NULL THEN charttime
                         ELSE chartdate 
                     END
                    AS base_term1
                  , CASE WHEN charttime IS NOT NULL THEN charttime + interval '72' hour
                         ELSE chartdate + interval '96' hour
                     END
                    AS end_term1

                  , CASE WHEN charttime IS NOT NULL THEN charttime - interval '24' hour
                         ELSE chartdate
                     END
                    AS base_term2 
                  , CASE WHEN charttime IS NOT NULL THEN charttime
                         ELSE chartdate + interval '24' hour
                     END
                    AS end_term2
"""

columns = """
    subject_id
    , hadm_id
    , icustay_id
    , intime
    , outtime
    , dbsource
    , antibiotic_startdate
    -- , antibiotic_enddate
    -- , antibiotic_name
    , drug_type
    -- , drug_name_generic
    -- , route
    -- , charttime
    -- , chartdate
    , charttime_
    -- , spec_type_desc
    -- , positive_culture
    , suspected_infection
    , suspected_infection_time
"""
