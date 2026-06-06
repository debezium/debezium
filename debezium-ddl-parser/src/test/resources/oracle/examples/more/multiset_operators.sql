-- Example from https://github.com/utPLSQL/utPLSQL/blob/3437b689e2097b1bdae4b0daf22b09549e5eabee/source/core/ut_suite_cache_manager.pkb
select c.obj.path as path
  from suite_items c
 where c.obj.tags multiset intersect :a_include_tag_list is not empty or :a_include_tag_list is empty;