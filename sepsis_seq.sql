select subject_id, string_agg(sepsis_expr, ',' order by admittime) as sepsis_seq
from (
    select adm.subject_id, adm.admittime
    -- , adm.diagnosis
    , concat(CASE
        WHEN s.explicit_sepsis = 1 THEN 'e'
        ELSE '' END,
        CASE
        WHEN s.infection = 1 AND s.organ_dysfunction = 1 THEN 'io'
        ELSE '' END,
        CASE
        WHEN s.infection = 1 AND s.mech_vent = 1 THEN 'im'
        ELSE '' END
    )
    AS sepsis_expr
    from admissions adm
        left join angus_sepsis as s
        on s.subject_id=adm.subject_id and s.hadm_id=adm.hadm_id
) t
where sepsis_expr <> ''
group by subject_id
;