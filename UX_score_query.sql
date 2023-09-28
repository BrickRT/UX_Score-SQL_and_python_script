select fracrash.usid, asi, device_id, custom_user_id, screen_duration, quit, fracrash.creation_date, type_id,rage_count,apiduration , apistatus, fracrash.crs, nonfatale, errorcount, app_launch_type, app_launch_time, apierrorcode,
user_defiened_event_tolerance, user_defiened_event_tolerance.name, user_defiened_event_frustrated, user_defiened_event_frustrated.name 
,case 
when (type_id = 22 and rage_count > 4) or apiduration > 6 or crs in  (1,2)  or apistatus = 'f' or (errorcount > 4)
or (app_launch_type = 'COLD' and app_launch_time > 4000) or (app_launch_type = 'HOT' and app_launch_time > 2000)
or apierrorcode is not null 
or user_defiened_event_frustrated is not null then 'fustrated'

when (type_id=22 and rage_count in (1,2,3)) or (apiduration between 1.5 and 6) or  (errorcount in (1,2,3))
or (app_launch_type = 'COLD' and app_launch_time between 1000 and 4000) or (app_launch_type = 'HOT' and app_launch_time between 500 and 2000)
or user_defiened_event_tolerance is not null then 'Tolerance'
else 'sucess'
end as 'case'
from(select ut.id usid, ut.exception_type  crs, ut.created_at creation_date
from user_tasks ut
where ut.app_id = 2619
 and ut.platform = 1 and ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00'
 )fracrash 
 
left outer join (
select uta.user_task_id, (uta.duration/1000) apiduration, uta.status_type apistatus
from user_tasks_api as uta
join user_tasks  as ut on ut.id = uta.user_task_id
where ut.app_id = 2619 and uta.duration > 0
and  ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00'
group by uta.user_task_id,uta.status_type) api on api.user_task_id = fracrash.usid

left outer join(
select  ueb.usr_task_id,ueb.error_type_id nonfatale, count(ueb.error_type_id) errorcount
from ue_events_base ueb
join user_tasks ut on ut.id = ueb.usr_task_id
where ut.app_id = 2619  and  ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00'
 and ueb.error_type_id is not null
group by ueb.usr_task_id, nonfatale
)Nonfatalexception on Nonfatalexception.usr_task_id = fracrash.usid

left outer join (
select  ueb.usr_task_id, ueb.type_id type_id,count(ueb.type_id) rage_count
from ue_events_base ueb
join user_tasks ut on ut.id = ueb.usr_task_id
where ut.app_id = 2619  and  ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00'
and ueb.type_id = 22
group by ueb.usr_task_id, ueb.type_id
)rage on rage.usr_task_id = fracrash.usid

left outer join (
select asi, device_id, custom_user_id, user_task_id, app_launch_type, app_launch_time 
from user_tasks_metadata utm
join user_tasks ut on ut.id = utm.user_task_id
where ut.app_id = 2619 and ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00'
) Applaunchtime on Applaunchtime.user_task_id = fracrash.usid

left outer join (
select uta.user_task_id, ss.status_code apierrorcode
from api_status_codes ss
join user_tasks_api uta on uta.status_code = ss.status_code
join user_tasks  as ut on ut.id = uta.user_task_id
where  ut.app_id = 2619  and  ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00' 
and ss.status_code in (500,404)
group by uta.user_task_id) apierror on apierror.user_task_id = fracrash.usid

left outer join (
select ueb.usr_task_id , ueb.app_event_id user_defiened_event_tolerance, uae.name
from ue_events_base ueb
join user_tasks as ut on ut.id = ueb.usr_task_id
join unique_app_event as uae on uae.id = ueb.app_event_id
where ueb.app_event_id in (20162019,20337338,20320111,20706827,20432511,20411785,20337615,20320119,20490701,20580157,20684686,20490805,20729284,20376051,20453379,17597517,
20728928,20714557,20729087) and ut.app_id = 2619 and  
ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00')
user_defiened_event_tolerance on user_defiened_event_tolerance.usr_task_id = fracrash.usid

left outer join (
select ueb.usr_task_id , ueb.app_event_id user_defiened_event_frustrated, uae.name
from ue_events_base ueb
join user_tasks as ut on ut.id = ueb.usr_task_id
join unique_app_event as uae on uae.id = ueb.app_event_id
where ueb.app_event_id in (20320085,20320224,20320175,20337423,20337485,20728782,20638284,20729038,20393142,20638300) and ut.app_id = 2619 and  
ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00')
user_defiened_event_frustrated on user_defiened_event_frustrated.usr_task_id = fracrash.usid

left outer join (
select screen_duration, user_task_id
from batch_records_staging brs
join user_tasks ut on ut.id = brs.user_task_id
where  ut.app_id = 2619  and  ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00'
) brs on brs.user_task_id = fracrash.usid

left outer join (
select usr_task_id, sum(is_quit) as quit
from heatmap_events hme
join user_tasks ut on ut.id = hme.usr_task_id
where  ut.app_id = 2619  and  ut.created_at between '2022-12-11 18:30:00' and '2022-12-18 18:30:00' 
group by usr_task_id
) heatmap on heatmap.usr_task_id = fracrash.usid
limit 5000000;