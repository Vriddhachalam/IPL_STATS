import duckdb

def flatten():

    match_inning_over_by_ball_query='''
    with matches as (
            select
                ROW_NUMBER() OVER() AS match_id,
                meta,
                info,
                innings --from all_cric.all_cric_data
            from
                ipl_cric.all_ipl_data
            where 1 = 1
        ), match_innings as (
            select
                match_id,
                innings,
                UNNEST (
                    from_json(innings, '["json"]')
                ) as split_innings
            from matches
            where
                1 = 1
        ),
        final_match_innings as (
            select
                match_id,
                ROW_NUMBER () OVER(
                    partition by match_id
                    order by
                        innings
                ) AS innings_nbr,
                from_json(innings, '["json"]') [innings_nbr] split_innings
            from
                match_innings
        ),
        match_inning_overs as (
            select
                match_id,
                innings_nbr,
                json_extract_string(split_innings, 'team') as team,
                UNNEST (
                    from_json(
                        json_extract(split_innings, 'overs'),
                        '["json"]'
                    )
                ) as each_overs
            from final_match_innings
            where
                1 = 1
        ),
        match_inning_over_by_ball as (
            select
                mi.match_id,
                mi.innings_nbr,
                mi.team, (
                    json_extract_string(each_overs, 'over'):: int + 1
                ) as over_count,
                /*from_json(json_extract(each_overs, 'deliveries'), '["json"]') all_deliveries,*/
                UNNEST (
                    from_json(
                        json_extract(each_overs, 'deliveries'),
                        '["json"]'
                    )
                ) as deliveries
            from match_inning_overs mi
            where 1 = 1
        ), ball_by_ball as (
            select
                info,
                match_id,
                innings_nbr,
                team,
                over_count,
                deliveries,
                ROW_NUMBER () OVER(
                    partition by match_id,
                    innings_nbr,
                    over_count
                    order by
                        deliveries
                ) AS ball_nbr,
                from_json(all_deliveries, '["json"]') [ball_nbr] ball_detail
            from
                match_inning_over_by_ball
            where 1 = 1
        ), batsman_score as (
            select
                b.match_id,
                b.innings_nbr,
                b.team,
                b.over_count,
                b.ball_nbr,
                json_extract_string(b.ball_detail, 'batter') as batsman,
                json_extract_string(b.ball_detail, 'bowler') as bowler,
                json_extract_string(b.ball_detail, 'non_striker') as non_striker,
                json_extract_string(
                    b.ball_detail,
                    '$.runs.batter'
                ):: int as runs_scored,
                json_extract_string(
                    b.ball_detail,
                    '$.runs.extras'
                ):: int as extras,
                json_extract_string(
                    b.ball_detail,
                    '$.wickets[0].kind'
                ) as Wicket_Kind,
                json_extract_string(
                    b.ball_detail,
                    '$.wickets[0].player_out'
                ) as Wicket_Player
            from ball_by_ball b
                inner join matches m on m.match_id = b.match_id
        ), batsman_score_agg as (
            select
                b.match_id,
                b.innings_nbr,
                b.team,
                b.batsman,
                sum(runs_scored) runs_scored
            from batsman_score b
            group by
                1,
                2,
                3,
                4
        ),
        ballz_by_ballz as(
            select
                m.info,
                mi.*,
                json_extract_string(m.info, '$.city') as city,
                json_extract_string(m.info, '$.dates[0]'):: DATE as dates,
                json_extract_string(m.info, '$.venue') as venue,
                json_extract_string(
                    m.info,
                    '$.event.match_number'
                ) as season_match_no,
                json_extract_string(m.info, '$.event.name') as event_name,
                json_extract_string(m.info, '$.toss.decision') as toss_decision,
                json_extract_string(m.info, '$.toss.winner') as toss_winner,
                json_extract_string(
                    m.info,
                    '$.player_of_match[0]'
                ) as player_of_match,
                json_extract_string(m.info, '$.outcome.by.runs') as won_by_runs,
                json_extract_string(
                    m.info,
                    '$.outcome.by.wickets'
                ) as won_by_wickets,
                json_extract_string(m.info, '$.outcome.winner') as winner,
                json_extract_string(m.info, '$.outcome.result') as result,
                json_extract_string(m.info, '$.season') as season,

    CASE
        WHEN m.info -> 'teams' ->> 0 = mi.team THEN m.info -> 'teams' ->> 1
        ELSE m.info -> 'teams' ->> 0
    END AS team_bowl
    from
        match_inning_over_by_ball mi
        inner join matches m on mi.match_id = m.match_id
    order by 1, 2
    )

    ,
    transformed_ball_by_ball as(
        select
            bz.*,
            json_extract_string(bz.deliveries, 'batter') as batter,
            json_extract_string(bz.deliveries, 'bowler') as bowler,
            json_extract_string(bz.deliveries, 'non_striker') as non_striker,
            json_extract_string(
                bz.deliveries,
                '$.runs.batter'
            ):: int as runs_batter,
            json_extract_string(
                bz.deliveries,
                '$.runs.extras'
            ):: int as runs_extras,
            json_extract_string(
                bz.deliveries,
                '$.runs.non_boundary'
            ):: varchar as runs_non_boundary,
            json_extract_string(bz.deliveries, '$.runs.total'):: int as runs_total,
            json_extract_string(
                bz.deliveries,
                '$.extras.wides'
            ):: int as extras_wides,
            json_extract_string(
                bz.deliveries,
                '$.extras.byes'
            ):: int as extras_byes,
            json_extract_string(
                bz.deliveries,
                '$.extras.legbyes'
            ):: int as extras_legbyes,
            json_extract_string(
                bz.deliveries,
                '$.extras.noballs'
            ):: int as extras_noballs,
            json_extract_string(
                bz.deliveries,
                '$.extras.penalty'
            ):: int as extras_penalty,
            json_extract_string(bz.deliveries, '$.wickets') as wickets,
            json_extract_string(
                bz.deliveries,
                '$.wickets[0].kind'
            ) as wickets_kind,
            json_extract_string(
                bz.deliveries,
                '$.wickets[0].player_out'
            ) as wickets_player_out,
            json_extract_string(
                bz.deliveries,
                '$.wickets[0].fielders'
            ) as wickets_fielders,
            json_extract_string(
                bz.deliveries,
                '$.wickets[0].fielders[0].name'
            ) as wickets_fielder_name,
            json_extract_string(
                bz.deliveries,
                '$.wickets[0].fielders[1].name'
            ) as wickets_fielder_2_name,
            json_extract_string(
                bz.deliveries,
                '$.wickets[0].fielders[2].name'
            ) as wickets_fielder_3_name,
            json_extract_string(
                bz.deliveries,
                '$.wickets[0].fielders[3].name'
            ) as wickets_fielder_4_name,

    json_extract_string(bz.deliveries, '$.review.by') as review_by,
    json_extract_string(
        bz.deliveries,
        '$.review.umpire'
    ) as review_umpire,
    json_extract_string(
        bz.deliveries,
        '$.review.batter'
    ) as review_batter,
    json_extract_string(
        bz.deliveries,
        '$.review.decision'
    ) as review_decision,
    json_extract_string(
        bz.deliveries,
        '$.review.umpires_call'
    ) as review_umpires_call,
    json_extract_string(
        bz.deliveries,
        '$.review.type'
    ) as review_type,
    json_extract_string(
        bz.deliveries,
        '$.replacements.role'
    ) as replacements_role,
    json_extract_string(
        bz.deliveries,
        '$.replacements.match'
    ) as replacements_match,
    from
        ballz_by_ballz bz
    )
    select
        bb.*,
        ROW_NUMBER() OVER (
            PARTITION BY match_id
            ORDER BY
                innings_nbr,
                over_count
        ) AS ball_number,
        1 AS balls,
        CASE
            WHEN (runs_batter = 4)
            and (runs_non_boundary is null) THEN 1:: INT
            ELSE 0:: INT
        END AS "4s",
        CASE
            WHEN (runs_batter = 6)
            and (runs_non_boundary is null) THEN 1:: INT
            ELSE 0:: INT
        END AS "6s",
        CASE
            WHEN runs_extras = 0 THEN 1:: INT
            ELSE 0:: INT
        END AS "batter_balls",
        CASE
            WHEN extras_wides != 0 THEN 1:: INT
            ELSE 0:: INT
        END AS "wide_count",
        CASE
            WHEN wickets_kind IN (
                'caught',
                'bowled',
                'lbw',
                'caught and bowled',
                'stumped',
                'hit wicket'
            ) THEN 1:: INT
            ELSE 0:: INT
        END AS bowler_wicket,
        CASE
            WHEN wickets_kind LIKE '%retired hurt%'
            or wickets_kind IS NULL THEN 0:: INT
            ELSE 1:: INT
        END AS wicket,
        balls - wide_count AS faced_balls,
        CASE
            WHEN wickets_kind = 'caught' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_caught,
        CASE
            WHEN wickets_kind = 'bowled' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_bowled,
        CASE
            WHEN wickets_kind = 'lbw' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_lbw,
        CASE
            WHEN wickets_kind = 'caught and bowled' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_caught_and_bowled,
        CASE
            WHEN wickets_kind = 'stumped' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_stumped,
        CASE
            WHEN wickets_kind = 'hit wicket' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_hit_wicket,
        CASE
            WHEN wickets_kind = 'run out' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_run_out,
        CASE
            WHEN wickets_kind = 'retired_hurt' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_retired_hurt,
        CASE
            WHEN wickets_kind = 'retired_out' THEN 1:: INT
            ELSE 0:: INT
        END AS wickets_kind_retired_out,

    from
        transformed_ball_by_ball bb
    order by
        dates,
        match_id,
        innings_nbr,
        over_count
        '''

    with duckdb.connect("ipl_cric.duckdb") as conn:


        if conn.sql("SELECT name FROM sqlite_master WHERE type='table' AND name='all_ipl_data';"):
            print("\njson files already loaded to database 'ipl_cric.duckdb'")

        else:
            conn.execute(''' CREATE OR REPLACE TABLE all_ipl_data AS
                            SELECT *  
                            FROM read_json('ipl_json/*.json',
                            --json_format = 'records',
                            records = true,
                            format='auto',                    
                            ignore_errors = false,
                            columns = {"meta":'JSON', "info": 'JSON', "innings": 'JSON'});
            ''')

            print("\njson files loaded to database 'ipl_cric.duckdb'")
            print("\ncreating duckdb connection to 'ipl_cric.duckdb'")
            print("\nflattening to ball_by_ball row format .......")

        if conn.sql("SELECT name FROM sqlite_master WHERE type='table' AND name='ball_by_ball_ipl';"):
            print('\njson files already flattened to ball_by_ball row format table')
        else:            
            conn.sql(f"  create or replace table ball_by_ball_ipl as ({match_inning_over_by_ball_query}) ;")
            print('\njson files flattened to ball_by_ball row format table')

