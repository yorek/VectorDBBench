create or alter procedure [$vector].[stp_filter_similar] 
@id int, 
@v vector(768),
@k int,
@m varchar(50) = 'cosine'
as
select top(@k)
    v.id
from 
    [benchmark].[vector_768] v
where
    v.id > @id
order by
    vector_distance(@m, @v, v.[vector])
GO
