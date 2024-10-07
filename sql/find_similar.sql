create or alter function [$vector].[find_similar$vector_768$vector] (@v vector(768), @k int, @p int, @d float, @m varchar(50) = 'cosine')
returns table
as return
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
    v.*,
    cosine_distance = vector_distance(@m, v.[vector], @v)
from 
    [$vector].[vector_768$vector$clusters] c 
inner join
    cteProbes k on k.cluster_id = c.cluster_id
inner join
        [benchmark].[vector_768] v on v.id = c.item_id
where
    vector_distance(@m, v.[vector], @v) <= @d
order by
    cosine_distance

