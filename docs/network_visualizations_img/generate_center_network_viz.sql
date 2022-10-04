--- Vibrant Routing

\c vibrant-routing

set role "vibrant-routing-role";

select current_user;

show timezone;

SET TIMEZONE='America/New_York';

show timezone;


set search_path to processed;

select count(*) from routing_table;

select * from routing_table limit 3;

select center1role, array_agg(distinct center1id) from routing_table group by 1 order by 1 asc;


\x

-- Are we sure that we need both?
select center1id, array_agg(distinct center1termination), cardinality(array_agg(distinct center1termination)), count(*) from routing_table  group by 1 order by 3 desc limit 10;


begin;

create table edges as (
select
center1id as from, center2id as to, '1 > 2'
from routing_table
union
select
center2id as from, center3id as to, '2 > 3'
from routing_table
order by 1
);

select count(*) from edges;

\a
\pset pager off
\pset tuples_only


-- Generates graphviz plot from table
\o rt.dot -- | dot -Tpng "rt.png"
with graphviz_edges as (
select ("from" || ' -> ' || "to") as edge
from edges
),
graphviz_nodes as (
select center1role,
'node [fillcolor="' || (array['lime', 'aqua', 'magenta', 'crimson', 'darkgrey', 'darkolivegreen1', 'x11purple', 'orange', 'sienna1', 'yellow2'])[floor(random() * 10 + 1)] || '"]; ' || string_agg(distinct center1id, ' ') || ' # ' || center1role as node
from routing_tablewhere center1role is not null
group by 1 order by 1 asc
)
select 'digraph rt {
       fontname="Helvetica,Arial,sans-serif"
       node [fontname="Helvetica,Arial,sans-serif"] graph [
		newrank = true,
		#nodesep = 0.3,
		#ranksep = 0.2,
		#overlap = true,
		splines = true,
	        ]
       edge [
		#arrowhead = none,
		#arrowsize = 0.5,
		labelfontname = "Ubuntu",
		weight = 6,
		style = "filled,setlinewidth(5)"
	]
       node [
		fixedsize = false,
		fontsize = 24,
		#height = 1,
		#shape = box,
		style = "filled,setlinewidth(5)",
		width = 2.2
	]
 ' || (select string_agg(node, ';
') from graphviz_nodes) || (select string_agg(edge, ';
') from graphviz_edges) || '}';
rollback;