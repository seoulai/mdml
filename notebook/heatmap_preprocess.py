import matplotlib.pyplot as plt
import seaborn as sns; sns.set(rc={'figure.figsize':(15,15)})
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import MinMaxScaler
engine = create_engine('postgresql://postgres:mimic@127.0.0.1:5555/mimic')

def plot_ts(df, tick_position='bottom', figsize=(12,6)):
    plt.subplots(figsize=figsize)
    
    cmap = sns.cm.rocket_r # 값이 클수록 진하게

    ax = sns.heatmap(df.T, vmin=-0.1, cmap=cmap)
    if tick_position == 'top':
        ax.set_xticklabels(ax.get_xticklabels(), rotation=90) # x틱 라벨 90도 회전
        ax.xaxis.set_ticks_position('top') # x 틱 그래프 위로 
    elif tick_position == 'bottom':
#         ax.set_xticklabels(ax.get_xticklabels(), rotation=90) # x틱 라벨 90도 회전
        ax.xaxis.set_ticks_position('bottom') # x 틱 그래프 위로 
    
def map_drug_group(drug_list):
    
    pass


def get_data(subject_id=363):
    adm_sql = """
    select 
        t1.subject_id
        , t1.ts
        , t1.category
        , t1.event_name
        , t1.event_value
    from (
        select 
            {subject_id} as subject_id
            , min(intime) as ts
            , 'admit' as category
            , 'icu_los' event_name
            , avg(icu_los) as event_value
        from sepsis3
        where hadm_id in (
            select hadm_id 
            from admissions
            where subject_id={subject_id}
        )
        group by icustay_id

        union all

        select 
            {subject_id} as subject_id
            , min(intime) as ts
            , 'admit' as category
            , 'hosp_los' event_name
            , avg(hosp_los) as event_value
        from sepsis3
        where hadm_id in (
            select hadm_id 
            from admissions
            where subject_id={subject_id}
        )
        group by hadm_id

        union all

        select 
            subject_id
            , deathtime as ts
            , 'admit' as category
            , 'died' as event_name
            , 1 as event_value
        from admissions
        where deathtime is not null
        and subject_id={subject_id}

    ) t1
    ;
    """.format(subject_id=subject_id)
    adm = pd.read_sql(adm_sql, engine)

    drug_sql = """
    select subject_id
        , startdate as ts
        , 'prescription' as category
        , concat(drug, ' ', dose_val_rx, ' ', dose_unit_rx) as event_name
        , (EXTRACT(EPOCH FROM enddate - startdate))/ 60 / 60 / 24 as event_value
    from prescriptions
    where subject_id={}
    """.format(subject_id)

    drug = pd.read_sql(drug_sql, engine)
#     d_drug_grp = map_drug_group(drug.event_name.unique())
#     drug.event_name = drug.event_name.map(d_drug_grp)

    lab_sql = """
    select subject_id, charttime as ts, 'lab' as category, label as event_name,
        case when flag = 'abnormal' then 1 else 0 end event_value
    from labevents join d_labitems using (itemid) 
    where subject_id={}
    """.format(subject_id)
    lab = pd.read_sql(lab_sql, engine)
    
    microbio = pd.read_sql("""
    select subject_id, charttime as ts, 'microbio' as category,
        concat(org_name, ' - ', ab_name) as event_name, dilution_value as event_value
    from microbiologyevents
    where spec_type_desc = 'BLOOD CULTURE'
        and lower(org_name) similar to '%%(pseudomonas|aeruginosa|staphylococcus|aureus|escherichia|coli)%%'
        and ab_itemid is not null
        and subject_id={}
    """.format(subject_id), engine)
    
    adm.event_value = adm.event_value.astype(np.float32)
    drug.event_value = drug.event_value.astype(np.float32)
    lab.event_value = lab.event_value.astype(np.float32)
    microbio.event_value = microbio.event_value.astype(np.float32)

    return adm, drug, lab, microbio

def normalize(df):
    v = df.values
    scaler = MinMaxScaler()
    scaler.fit(v)
    return pd.DataFrame(scaler.transform(v), columns=df.columns, index=df.index)
    
def ts_pivot(subject_id=363, period_agg=None):
    adm, drug, lab, microbio = get_data(subject_id)
    adm_pivot = pd.pivot_table(adm, 
                           index='ts', 
                           columns=['category', 'event_name'], 
                           values='event_value', 
                           aggfunc=np.sum)
    drug_pivot = pd.pivot_table(drug, 
                           index='ts', 
                           columns=['category', 'event_name'], 
                           values='event_value', 
                           aggfunc=np.sum)
    lab_pivot = pd.pivot_table(lab, 
                           index='ts', 
                           columns=['category', 'event_name'], 
                           values='event_value', 
                           aggfunc=np.sum)
    microbio_pivot = pd.pivot_table(microbio, 
                           index='ts', 
                           columns=['category', 'event_name'], 
                           values='event_value', 
                           aggfunc=np.sum)
    
    if period_agg:
        adm_pivot = adm_pivot.resample(period_agg, convention='start').sum()
        drug_pivot = drug_pivot.resample(period_agg, convention='start').sum()
        lab_pivot = lab_pivot.resample(period_agg, convention='start').sum()
        microbio_pivot = microbio_pivot.resample(period_agg, convention='start').sum()
    return normalize(adm_pivot), normalize(drug_pivot), normalize(lab_pivot), normalize(microbio_pivot)
    