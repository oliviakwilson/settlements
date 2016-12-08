#!/usr/bin/python

# Created 03 November, 2016
# Analyst: Olivia Wilson
# Purpose: Combine Atlas and NE2 settlement data and perform continuous contract calculations, then insert into settlements MySQL database table

import MySQLdb                                              # To interact with MySQL
import time                                                 # For script tracking purposes
from datetime import date, timedelta                        # For data date insertion
import os                                                   # Housekeeping... specifically, for renaming a file     

# Start of the ETL
start_time = time.time()
print ("Program started at %s ") % (start_time)

# Relevant Dates
yesterday = date.today() - timedelta(1)
yesterday_fmt = yesterday.strftime('%Y-%m-%d')

# initial load range
init_range = range(1, 2)

# New Database Connection Details
db = MySQLdb.connect(   host="",           # Host
                        user="",           # Username
                        passwd="",         # Password
                        db=""              # Name of Database
                         )
try:
   cur = db.cursor()
   print ('Successfully connected to database')
except:
   print ('Connection unsuccessful')


# Note to self: 13 instances of Settlement date in where clauses
# SQL String for insertion into settlements table
sql_str = """

replace into warehouse.settlements (settlementdate, market, basis, sourcename, prompt_month, month_2, month_3, prompt_settlement, month_2_settlement, month_3_settlement, prompt_month_basis, month_2_basis, month_3_basis, prompt_CL, month_2_CL, month_3_CL, prompt_commodity_flat, month_2_commodity_flat, month_3_commodity_flat, prompt_commodity_CL_diff, month_2_commodity_CL_diff, month_3_commodity_CL_diff)

(

/* Main Select Statement: Post All Calculations */
select
    f.settlementDate,
    f.market,
    f.basis,
    f.sourcename,
    f.prompt_month,
    f.month_2,
    f.month_3,
    f.prompt_set,
    f.month_2_set,
    f.month_3_set,
    f.prompt_month_basis,
    f.month_2_basis,
    f.month_3_basis,
    f.prompt_CL_flat,
    f.month_2_CL_flat,
    f.month_3_CL_flat,
    case
        when f.prompt_set = 0 then 0
        else (f.prompt_set + f.prompt_month_basis) 
    end as prompt_commodity_flat,
    case 
        when f.month_2_set = 0 then 0
        else (f.month_2_set + f.month_2_basis) 
    end as month_2_commodity_flat,
    case 
        when f.month_3_set = 0 then 0
        else (f.month_3_set + f.month_3_basis) 
    end as month_3_commodity_flat,
    case
        when f.prompt_set = 0 then 0
        else ((f.prompt_set + f.prompt_month_basis) - f.prompt_CL_flat) 
    end as prompt_commodity_cl_diff,
    case 
        when f.month_2_set = 0 then 0
        else ((f.month_2_set + f.month_2_basis) - f.month_2_CL_flat) 
    end as month_2_commodity_cl_diff,
    case 
        when f.month_3_set = 0 then 0
        else ((f.month_3_set + f.month_3_basis) - f.month_3_CL_flat) 
    end as month_3_commodity_cl_diff

from (

/* Settlement Values are aggregated at this level and null values removed from Basis*/    
select distinct
    a.settlementDate,
    a.market,
    a.basis,
    a.sourcename,
    a.prompt_month,
    a.month_2,
    a.month_3,
    sum(a.prompt_value) as prompt_set,
    sum(a.month_2_value) as month_2_set,
    sum(a.month_3_value) as month_3_set,
    ifnull(a.prompt_month_basis, '') as prompt_month_basis,
    ifnull(a.month_2_basis, '') as month_2_basis,
    ifnull(a.month_3_basis, '') as month_3_basis,
    a.prompt_CL_flat,
    a.month_2_CL_flat,
    a.month_3_CL_flat
    
from
/* Atlas, NE2 and WTI-CMA Differential Row are unioned here */

(
select distinct
    m.settlementdate,
    case
        when m.Market like '%%Cliff%%' then 'WCL-Cushing'
        when m.Market like '%%WCS-Cush%%' then 'WCS-Cushing'
        when m.Market like '%%CLK-Cush%%' then 'CLK-Cushing'
        when m.Market like '%%CLK-Pat%%' then 'CLK-Patoka'
        when m.Market = 'MEH' then 'WTI-MEH'
        when m.Market = 'MID' then 'WTI-MID'
        else m.Market
    end as Market,
    m.basis,
    m.sourcename,
    m.prompt_month,
    m.month_2,
    m.month_3,
    m.prompt_month_value as prompt_value,
    m.month_2_value as month_2_value,
    m.month_3_value as month_3_value,
    flat.prompt_flat as prompt_month_basis,
    flat.month_2_flat as month_2_basis,
    flat.month_3_flat as month_3_basis,
    cl.prompt_CL_flat,
    cl.month_2_CL_flat,
    cl.month_3_CL_flat
from
(
/* Atlas Data */
select distinct
    s.settlementdate,
    case
        when s.p_market = '' or s.p_market is null then s.Market
        else concat(s.Market, "-", s.p_market)
    end as market, -- This combines the market and the parent market into single value
    case
            when s.Basis = 'WTI-CMA' then 'CS'
            when s.Basis = 'WTI' then 'CL'
            else s.Basis
    end as basis, -- this normalises the basis so that it is universal for all data sources
    s.sourcename,
    months.prompt_month_char,
    months.month_2_char,
    months.month_3_char,
    months.prompt_month,
    months.month_2,
    months.month_3,
    -- These series of case statements match the instrument to the desired month m1, m2, m3 and generate the appropriate settlement value for the commodity in question
    case
        when s.instrument = months.prompt_month then s.settlementvalue
        else 0
    end as prompt_month_value,
    case
        when s.instrument = months.month_2 then s.settlementvalue
        else 0
    end as month_2_value, 
    case
        when s.instrument = months.month_3 then s.settlementvalue
        else 0
    end as month_3_value
from
    warehouse.settlement_atlas_web_scrape_daily s
        left join
            -- This join ensures that the roll month matches the atlas data set and designates the instrument value for m1, m2, m3
            (
            select 
                settlementdate,
                market,
                min(Instrument) as prompt_month,
                date_add(min(Instrument), interval 1 month) as month_2,
                date_add(min(Instrument), interval 2 month) as month_3,
                date_format(min(Instrument), '%%b-%%y') as prompt_month_char,
                date_format(date_add(min(Instrument), interval 1 month), '%%b-%%y') as month_2_char,
                date_format(date_add(min(Instrument), interval 2 month), '%%b-%%y') as month_3_char
            from 
                warehouse.settlement_atlas_web_scrape_daily
			where
				settlementdate = current_date() - interval %s day
            group by
                settlementdate,
                market
            order by
                settlementdate,
                market,
                prompt_month
                ) months
            on s.settlementdate = months.settlementdate
            and s.market = months.market
where
	s.settlementdate = current_date() - interval %s day -- This will change according to the settlementdate desired
group by
    s.settlementdate,
    case
        when s.p_market = '' or s.p_market is null then s.Market
        else concat(s.Market, "-", s.p_market)
    end,
    s.basis,
    s.sourcename,
    months.prompt_month_char,
    months.month_2_char,
    months.month_3_char,
    case
        when s.instrument = months.prompt_month then s.settlementvalue
        else 0
    end,
    case
        when s.instrument = months.month_2 then s.settlementvalue
        else 0
    end, 
    case
        when s.instrument = months.month_3 then s.settlementvalue
        else 0
    end


union

/* NE2 Data */
select
    s.settlementdate,
    s.market,
    i.nymex_basis as basis,
    'NE2' as sourcename,
    months.prompt_month_char,
    months.month_2_char,
    months.month_3_char,
    months.prompt_month,
    months.month_2,
    months.month_3,
    -- This mirrors the calculations performed for the Atlas data
    sum(case
        when s.InstrumentStartDate = months.prompt_month then s.settlementvalue
        else 0
    end) as prompt_month_value,
    sum(case
        when s.InstrumentStartDate = months.month_2 then s.settlementvalue
        else 0
    end) as month_2_value, 
    sum(case
        when s.InstrumentStartDate = months.month_3 then s.settlementvalue
        else 0
    end) as month_3_value
from
    warehouse.settlement_ne2_web_scrape_daily s
    -- The basis is found in the ne2 index
        left join warehouse.settlement_ne2_index i
            on  s.Market = i.ne2_commodity
        left join 
             (
            select distinct
                settlementdate,
                min(Instrument) as prompt_month,
                date_add(min(Instrument), interval 1 month) as month_2,
                date_add(min(Instrument), interval 2 month) as month_3,
                date_format(min(Instrument), '%%b-%%y') as prompt_month_char,
                date_format(date_add(min(Instrument), interval 1 month), '%%b-%%y') as month_2_char,
                date_format(date_add(min(Instrument), interval 2 month), '%%b-%%y') as month_3_char
            from 
                warehouse.settlement_atlas_web_scrape_daily
			where
				settlementdate = current_date() - interval %s day
            group by
                settlementdate
            order by
                settlementdate,
                prompt_month
                ) months
            on s.settlementdate = months.settlementdate
where
	s.settlementdate = current_date() - interval %s day
    and length(s.Instrument) < 7
group by
    s.settlementdate,
    s.market,
    basis,
    months.prompt_month_char,
    months.month_2_char,
    months.month_3_char,
    months.prompt_month,
    months.month_2,
    months.month_3
    
union

/* Union Part 3: Data to achieve WTI-CMA differential Row - underlying data from NE2 -- CL_CS Diff */
select distinct
    clcs.SettlementDate
    , 'WTI-CMA Diff' as Market
    , 'CS' as Basis
    , 'NE2' as SourceName
    , months.prompt_month_char
    , months.month_2_char
    , months.month_3_char
    , months.prompt_month
    , months.month_2
    , months.month_3
    , sum(case
        when clcs.InstrumentStartDate = months.prompt_month then (clcs.CLFlatPrice - clcs.CSFlatPrice)
        else 0
    end) as prompt_month_value
    , sum(case
        when clcs.InstrumentStartDate = months.month_2 then (clcs.CLFlatPrice - clcs.CSFlatPrice)
        else 0
    end) as month_2_value
    , sum(case
        when clcs.InstrumentStartDate = months.month_3 then (clcs.CLFlatPrice - clcs.CSFlatPrice)
        else 0
    end) as month_3_value
from
    (                        
        select
            SettlementDate
            , Instrument
            , case
                when Market = 'CL' then SettlementValue 
                else 0
            end as CLFlatPrice
            , case
                when Market = 'CS' then SettlementValue
                else 0
            end as CSFlatPrice
            , InstrumentStartDate
            , InstrumentEndDate
        from
            warehouse.settlement_ne2_web_scrape_daily
        where
            Market in ('CL', 'CS')
            and settlementdate = current_date() - interval %s day
            and length(Instrument) < 7
     )clcs
    left join 
             (
            select distinct
                settlementdate,
                min(Instrument) as prompt_month,
                date_add(min(Instrument), interval 1 month) as month_2,
                date_add(min(Instrument), interval 2 month) as month_3,
                date_format(min(Instrument), '%%b-%%y') as prompt_month_char,
                date_format(date_add(min(Instrument), interval 1 month), '%%b-%%y') as month_2_char,
                date_format(date_add(min(Instrument), interval 2 month), '%%b-%%y') as month_3_char
            from 
                warehouse.settlement_atlas_web_scrape_daily
			where
				settlementdate = current_date() - interval %s day
            group by
                settlementdate
            order by
                settlementdate,
                prompt_month
                ) months
            on clcs.settlementdate = months.settlementdate
where
	clcs.settlementdate = current_date() - interval %s day
group by
    clcs.SettlementDate
    , months.prompt_month_char
    , months.month_2_char
    , months.month_3_char
    , months.prompt_month
    , months.month_2
    , months.month_3



    ) m
    
/* Basis Calculations Prompt, Month 1, Month 2 */
left join
    (select
        s.SettlementDate,
        s.Market as Basis,
        sum(case
            when s.InstrumentStartDate = months.prompt_month then s.SettlementValue
            else 0
        end) as prompt_flat,
        sum(case
            when s.InstrumentStartDate = months.month_2 then s.SettlementValue
            else 0
        end) as month_2_flat,
        sum(case
            when s.InstrumentStartDate = months.month_3 then s.SettlementValue
            else 0
        end) as month_3_flat
    from
        warehouse.settlement_ne2_web_scrape_daily s
            left join 
             (
            select distinct
                settlementdate,
                min(Instrument) as prompt_month,
                date_add(min(Instrument), interval 1 month) as month_2,
                date_add(min(Instrument), interval 2 month) as month_3,
                date_format(min(Instrument), '%%b-%%y') as prompt_month_char,
                date_format(date_add(min(Instrument), interval 1 month), '%%b-%%y') as month_2_char,
                date_format(date_add(min(Instrument), interval 2 month), '%%b-%%y') as month_3_char
            from 
                warehouse.settlement_atlas_web_scrape_daily
			where
				settlementdate = current_date() - interval %s day
            group by
                settlementdate
            order by
                settlementdate,
                prompt_month
                ) months
            on s.settlementdate = months.settlementdate
    where
        s.Market in (
                    select distinct 
                        ne2_commodity
                    from 
                        warehouse.settlement_ne2_index
                    where 
                        nymex_basis = '' 
                        or nymex_basis is null
                    )
        and length(s.Instrument) < 7 -- excludes differentials like 'Oct-16/Jan-17 and ensures only flat price'
        and s.InstrumentStartDate < date_add(s.SettlementDate, interval 4 month)
        and s.settlementdate = current_date() - interval %s day
    group by
        s.SettlementDate,
        s.Market
    )flat
        on m.settlementDate = flat.settlementDate
        and m.basis = flat.Basis
        
/* CL Flat Price */
left join (       
    select distinct
        cl.SettlementDate
        , sum(case
            when cl.InstrumentStartDate = months.prompt_month then cl.CLFlatPrice
            else 0
        end) as prompt_CL_flat
        , sum(case
            when cl.InstrumentStartDate = months.month_2 then cl.CLFlatPrice
            else 0
        end) as month_2_CL_flat
        , sum(case
            when cl.InstrumentStartDate = months.month_3 then cl.CLFlatPrice
            else 0
        end) as month_3_CL_flat
    from
        (                        
        select
            cl.SettlementDate
            , cl.market
            , cl.InstrumentStartDate
            , case
                when cl.Market = 'CL' then SettlementValue 
                else 0
            end as CLFlatPrice   
        from
            warehouse.settlement_ne2_web_scrape_daily cl
        where
            cl.Market = 'CL'
            and length(cl.Instrument) < 7 -- excludes differentials like 'Oct-16/Jan-17 and ensures only flat price'
            and cl.settlementdate = current_date() - interval %s day
        )cl
         left join 
             (
            select distinct
                settlementdate,
                min(Instrument) as prompt_month,
                date_add(min(Instrument), interval 1 month) as month_2,
                date_add(min(Instrument), interval 2 month) as month_3,
                date_format(min(Instrument), '%%b-%%y') as prompt_month_char,
                date_format(date_add(min(Instrument), interval 1 month), '%%b-%%y') as month_2_char,
                date_format(date_add(min(Instrument), interval 2 month), '%%b-%%y') as month_3_char
            from 
                warehouse.settlement_atlas_web_scrape_daily
			where
				settlementdate = current_date() - interval %s day
            group by
                settlementdate
            order by
                settlementdate,
                prompt_month
                ) months
            on cl.settlementdate = months.settlementdate
    group by
        cl.SettlementDate
    order by
        cl.SettlementDate
    )cl
        on m.settlementDate = cl.settlementDate
where
	m.settlementdate = current_date() - interval %s day
group by
    m.settlementdate,
    case
        when m.Market like '%%Cliff%%' then 'WCL-Cushing'
        when m.Market like '%%WCS-Cush%%' then 'WCS-Cushing'
        when m.Market like '%%CLK-Cush%%' then 'CLK-Cushing'
        when m.Market like '%%CLK-Pat%%' then 'CLK-Patoka'
        when m.Market = 'MEH' then 'WTI-MEH'
        when m.Market = 'MID' then 'WTI-MID'
        else m.Market
    end,
    m.basis,
    m.sourcename,
    m.prompt_month,
    m.month_2,
    m.month_3,
    m.prompt_month_value,
    m.month_2_value,
    m.month_3_value,
    cl.prompt_CL_flat,
    cl.month_2_CL_flat,
    cl.month_3_CL_flat
    
order by
    case
        when m.Market like '%%Cliff%%' then 'WCL-Cushing'
        when m.Market like '%%WCS-Cush%%' then 'WCS-Cushing'
        when m.Market like '%%CLK-Cush%%' then 'CLK-Cushing'
        when m.Market like '%%CLK-Pat%%' then 'CLK-Patoka'
        when m.Market = 'MEH' then 'WTI-MEH'
        when m.Market = 'MID' then 'WTI-MID'
        else m.Market
    end,
    m.basis,
    m.settlementdate,
    m.sourcename
    
    )a
where
	a.settlementdate = current_date() - interval %s day
group by
    a.settlementDate,
    a.market,
    a.basis,
    a.sourcename,
    a.prompt_month,
    a.month_2,
    a.month_3,
    a.prompt_month_basis,
    a.month_2_basis,
    a.month_3_basis,
    a.prompt_CL_flat,
    a.month_2_CL_flat,
    a.month_3_CL_flat 
    ) f 
); """

x = 0
for dt in init_range:
    # Pass in the parameters into the query
    sql_str_var = sql_str % (dt, dt, dt, dt, dt, dt, dt, dt, dt, dt, dt, dt, dt)
    # Execute the Query
    cur.execute(sql_str_var)
    # Collect the results
    fields = cur.fetchall()
    db.commit()
    x += 1
    print ("Inserted %s of %s records") % (x, max(init_range))

print "Inserted new continuous contract settlement records into the database "

# Close the connection to the database
cur.close()
print ('Closed the connection')

# Complete this SoB
print('This ETL is now complete')
print('The program took %s seconds to execute') % (time.time()-start_time)
print ("This program took %s minutes to execute") % ((time.time()-start_time)/60)