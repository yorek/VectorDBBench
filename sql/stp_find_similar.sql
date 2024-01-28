SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create or alter procedure [$vector].[stp_find_similar$vector_768$vector] 
@v varbinary(8000),
@k int,
@p int, 
@m varchar(50) = 'cosine'
as
with cteProbes as
(
    select top (@p)
        k.cluster_id
    from 
        [$vector].[vector_768$vector$clusters_centroids] k
    order by
        vector_distance(@m, k.[centroid], @v) 
)
select top(@k)
    v.id
from 
    [$vector].[vector_768$vector$clusters] c 
inner join
    cteProbes k on k.cluster_id = c.cluster_id
inner join
    [benchmark].[vector_768] v on v.id = c.item_id
order by
    vector_distance(@m, v.[vector], @v)
        
GO
