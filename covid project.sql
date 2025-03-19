/* data exploration */

select location, date, total_cases, new_cases, total_deaths, population
from deaths
order by 1,2;


-- total cases vs total deaths
select 
	continent,
	location,
	date, 
	total_cases, 
	total_deaths, 
	(total_deaths/total_cases)*100 as death_percentage
from deaths
where 
	(total_deaths/total_cases)*100 > 5
order by 2;



-- total cases vs population
select 
	continent,
	location,
	date, 
	population,
	total_cases, 
	(total_cases/population)*100 as death_percentage
from deaths
where continent='Asia'
order by location, date;



-- countries with highest infection rate compared to population
select
	location,
	population,
	max(total_cases) as highest_infection_count,
	max((total_cases/population)*100) as population_infected_percentage
from deaths
group by location, population
order by population_infected_percentage desc;



-- highest death count per population
/* by continent */
select
	continent,
	max(total_deaths) as total_death_count
from deaths
where 
	continent is not null
	and total_deaths is not null
group by continent
order by total_death_count desc;


/* by countries */
select
	location,
	max(total_deaths) as total_death_count
from deaths
where 
	continent is not null
	and total_deaths is not null
group by location
order by total_death_count desc;



-- global numbers

/* deaths by cases */
select 
	date, 
	sum(new_cases) as total_cases,
	sum(new_deaths) as total_deaths,
	(sum(new_deaths))/(sum(new_cases))*100 as death_percentage
from deaths
where continent is not null
group by date
order by 1,2;



/* join deaths and vaccinations table into one */
select *
from deaths dea
join vaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date;



-- total population vs vaccinations
select distinct
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations
from deaths dea
join vaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
order by 2,3;


-- new vaccinations summed up by each date and location
select distinct
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	vac.new_vaccinations,
	sum(vac.new_vaccinations) over (partition by dea.location 
	order by dea.location, dea.date) as rolling_people_vaccinated
	--this sums up the new vaccinations by each location for each new date
from deaths dea
join vaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
order by 2,3;



-- using CTE so dont have to repeat code
with vac_agg as (
-- aggregate new_vaccinations per location and date
select
	location,
	date,
	sum(distinct new_vaccinations) as new_vaccinations -- uses distinct to make sure there are no duplicates
from vaccinations
group by location, date
)
, pops_vac (continent, location, date, population, new_vaccinations, rolling_people_vaccinated)
as (
select -- no distinct as already aggregated
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	coalesce(vac_agg.new_vaccinations, 0) as new_vaccinations,
	sum(vac_agg.new_vaccinations) over (partition by dea.location 
	order by dea.date rows between unbounded preceding and current row) as rolling_people_vaccinated
	--this sums up the new vaccinations by each location for each new date
from deaths dea
left join vac_agg
	on dea.location = vac_agg.location
	and dea.date = vac_agg.date
where dea.continent is not null
)
select *, (rolling_people_vaccinated::NUMERIC/nullif(population, 0))*100
from pops_vac;


--------------------------------------------------------------------------

-- creating a new table 
create table percent_population_vaccinated 
(
continent varchar(255),
location varchar(255),
date date,
population numeric,
new_vaccinations numeric,
rolling_people_vaccinated numeric
);

with vac_agg as (
-- aggregate new_vaccinations per location and date
select
	location,
	date,
	sum(distinct new_vaccinations) as new_vaccinations -- uses distinct to make sure there are no duplicates
from vaccinations
group by location, date
)
insert into percent_population_vaccinated
select -- no distinct as already aggregated
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	coalesce(vac_agg.new_vaccinations, 0) as new_vaccinations,
	sum(vac_agg.new_vaccinations) over (partition by dea.location 
	order by dea.date rows between unbounded preceding and current row) as rolling_people_vaccinated
	--this sums up the new vaccinations by each location for each new date
from deaths dea
left join vac_agg
	on dea.location = vac_agg.location
	and dea.date = vac_agg.date
where dea.continent is not null;

select *, (rolling_people_vaccinated/population)*100
from percent_population_vaccinated;


--------------------------------------------------------------------

-- creating View to store data 
create view percentage_population_vaccinated as
with vac_agg as (
-- aggregate new_vaccinations per location and date
select
	location,
	date,
	sum(distinct new_vaccinations) as new_vaccinations -- uses distinct to make sure there are no duplicates
from vaccinations
group by location, date
)
select -- no distinct as already aggregated
	dea.continent,
	dea.location,
	dea.date,
	dea.population,
	coalesce(vac_agg.new_vaccinations, 0) as new_vaccinations,
	sum(vac_agg.new_vaccinations) over (partition by dea.location 
	order by dea.date rows between unbounded preceding and current row) as rolling_people_vaccinated
	--this sums up the new vaccinations by each location for each new date
from deaths dea
left join vac_agg
	on dea.location = vac_agg.location
	and dea.date = vac_agg.date
where dea.continent is not null;


select * from percentage_population_vaccinated;

