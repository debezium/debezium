alter type my_type add attribute my_new_attr varchar2(200) invalidate;

alter type my_type modify attribute my_new_attr varchar2(400) cascade;
alter type my_type modify attribute my_new_attr varchar2(800);

alter type my_type drop attribute my_new_attr invalidate;
