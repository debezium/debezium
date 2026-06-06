select
    case when doc is JSON then 'valid' else 'invalid' end
    from persons p;

       