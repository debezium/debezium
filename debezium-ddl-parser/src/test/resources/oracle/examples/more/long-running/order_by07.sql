create or replace view testview
(
 id, --Test comment
    report_id,
    report_element_id,
    parent_id,
    chapterNo,
    sequenceNo,
    title_de,
    title_en,
    is_direct_restricted,
    is_inherited_restricted,
    has_note,
    current_status_code,
    report_element_discriminator,
    has_childs,
    validation_needed,
 -- this is required that ef6 can use this view with a primary key!
 constraint v_report_tree primary key (id) rely disable
)
as 
with
    restr as (
        select 
            entityid reid
            , max(isdirectrestricted) isDirectRestricted
            , case when min(isdirectrestricted) = 0 then 1 else 0 end isIndirectRestricted
        from 
            v_report_element_restrictions 
        where restrictionstart < sysdate and restrictionend > sysdate
        group by
            entityid
    ),
    notes as (
        SELECT
            report_element_id,
            1 hasNote
        FROM
            report_element_note
        where
            deleted = 0
        group by
            report_element_id    
    ),
    vaidationNeeded as (
        SELECT
            report_element_id
            , 1 ValidationNeeded
        FROM
            v_formatted_data_item_value_usage
        WHERE
            validationstatus = 'notvalidated'
        group by 
            report_element_id    
    ),
    contentText as (
        select
            *
        from
        (
                select 
                    re.id report_element_id
                    , lang.code
                    , CASE
                        WHEN re.discriminator = 'DISCRIMA' THEN to_char('[' || SUBSTR(Regexp_replace(ce.text, '<.+?>'),0,30) || '...]')
                        ELSE to_char(Regexp_replace(ce.text, '<.+?>')) end text
                from
                    content_element ce
                        inner join content c on c.id = ce.content_id
                        inner join language lang on lang.id = ce.language_id        
                        inner join report_element re on c.id = nvl(re.text_content_id,re.title_content_id)
                where
                    c.deleted = 0
                    and ce.deleted = 0
                order by
                    re.id, ce.content_id
        )            
        PIVOT  (min(Text) AS lang_text FOR (code) IN ('EY' AS DE, 'OH' AS EN))           
    )
select
 SYS_GUID() id
    , re.report_id report_id
    , re.id report_element_id
    , re.parent_id parent_id
    , report_helper.ReportElementNo(re.id) chapterNo
    , re.sequence_no sequenceNo
    , ct.de_lang_text title_de
    , ct.en_lang_text title_en
    , cast(nvl(restr.isDirectRestricted,0) as number(1)) is_direct_restricted
    , cast(nvl(restr.isIndirectRestricted,0) as number(1)) is_inherited_restricted
    , cast(nvl(notes.hasNote,0) as number(1)) has_note
    , re.status_code current_status_code
    , re.discriminator report_element_discriminator
    , cast((case when exists(select 1 from report_element re2 where re2.parent_id = re.id and deleted = 0) then 1 else 0 end) as number(1)) has_childs
    , cast(nvl(valNeed.ValidationNeeded,0) as number(1)) validation_needed
from 
    report_element re
    left outer join contentText ct on ct.report_element_id = re.id   
    left outer join restr restr on restr.reid = re.id
    left outer join notes notes on notes.report_element_id = re.id
    left outer join vaidationNeeded valNeed on valNeed.report_element_id = re.id
where 
    re.deleted = 0
start with re.parent_id is null
connect by prior re.id = re.parent_id
order siblings by sequenceno;
