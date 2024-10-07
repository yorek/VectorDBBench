create or alter procedure [$vector].[stp_find_similar$vector_768$vector] 
@v vector(768),
@k int,
@m varchar(50) = 'cosine'
as
select top(@k)
    v.id
from 
    [benchmark].[vector_768] v 
order by
    vector_distance(@m, @v, v.[vector])
GO
