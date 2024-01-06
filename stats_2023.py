# %%


def charts():

    import duckdb
    import pandas as pd
    import json
    import random
    import plotly.graph_objects as go
    import plotly.io as pio
    import plotly.offline as pyo
    import plotly.express as px
    import os

# for file_name in files_in_folder:
#     file_path = os.path.join(folder_path, file_name)
    file_path= os.getcwd()+'template_2023'+'source/'
    if os.path.exists(file_path):

        print("\n Generataing plots....")
        # %%
        with duckdb.connect("ipl_cric.duckdb") as conn:
            conn.sql('show tables')
            match_inning_over_by_ballz=conn.sql('select * from ball_by_ball_ipl').df()
        # conn.close()

        # %%
        # match_inning_over_by_ballz

        # %%
        # match_inning_over_by_ballz['season'].value_counts()

        # %%
        def season_update(x):
            if x=='2020/21':
                return '2020'
            elif x=='2009/10':
                return '2010'
            elif x=='2007/08':
                return '2008'
            else:
                return x

        # %%
        match_inning_over_by_ballz['season']=match_inning_over_by_ballz['season'].apply(lambda x:season_update(x))

        # %%
        match_inning_over_by_ballz['venue'].value_counts().reset_index().sort_values('venue')

        # %%
        match_inning_over_by_ballz['venue']=match_inning_over_by_ballz['venue'].apply(lambda x:x.split(',')[0])

        # %%
        match_inning_over_by_ballz['venue'].value_counts().reset_index().sort_values('venue')

        # %%
        def venue_update(x):
            if x=="Punjab Cricket Association Stadium":
                return "Punjab Cricket Association IS Bindra Stadium"
            elif x=="M.Chinnaswamy Stadium":
                return "M Chinnaswamy Stadium"
            else:
                return x   

        # %%
        match_inning_over_by_ballz['venue']=match_inning_over_by_ballz['venue'].apply(lambda x:venue_update(x))

        # %%
        match_inning_over_by_ballz['venue'].value_counts().reset_index().sort_values('venue')

        # %%
        match_inning_over_by_ballz['city'].value_counts().reset_index().sort_values('city')

        # %%
        match_inning_over_by_ballz['city']=match_inning_over_by_ballz['city'].apply(lambda x:'Bengaluru' if x=='Bangalore' else x)

        # %%
        match_inning_over_by_ballz['city'].value_counts().reset_index().sort_values('city')

        # %%
        match_inning_over_by_ballz.groupby('batter').sum(numeric_only=True)

        # %%
        match_inning_over_by_ball=match_inning_over_by_ballz.copy()
        match_inning_over_by_ball['dates']=match_inning_over_by_ball['dates'].apply(lambda x:str(x))

        highest_score=pd.DataFrame(match_inning_over_by_ball.groupby(by=['batter','season','match_id']).sum(numeric_only=True)).groupby(by=['batter','season']).max().sort_values(by='runs_batter',ascending=False)['runs_batter']
        highest_score=highest_score.reset_index()
        highest_score.columns=['batter','season','HS']
        # # highest_score[0:10]

        # highest_score=pd.DataFrame(match_inning_over_by_ball.groupby(by=['batter','match_id']).sum(numeric_only=True)['runs_batter']).groupby(by='season').max().sort_values(by='runs_total',ascending=False)
        # highest_score.columns=['HS']


        set_runs=pd.DataFrame(match_inning_over_by_ball.groupby(by=['batter','match_id','season']).sum(numeric_only=True))
        set_runs['100s']=set_runs['runs_batter'].apply(lambda x:1 if x>99 else 0)
        set_runs['50s']=set_runs['runs_batter'].apply(lambda x:1 if (x<100 and x>49) else 0)
        set_runs=set_runs[['50s','100s']].groupby(by=['batter','season']).sum(numeric_only=True)
        # set_runs[0:10]

        duck=match_inning_over_by_ball.groupby(by=['batter','match_id','season']).sum(numeric_only=True)
        def duck_out(x):
            if x[0]==0 and x[1]==1:
                return 1
            else:
                return 0

        duck['duck_out']=duck[['runs_total','wicket']].apply(duck_out,axis=1)
        duck=duck.groupby(by=['batter','season']).sum(numeric_only=True)['duck_out']
        # duck[0:10]

        not_out=pd.DataFrame(match_inning_over_by_ball.groupby(by=['batter','match_id','season']).sum(numeric_only=True))
        not_out['not_out']=not_out['wicket'].apply(lambda x:1 if x==0 else 0)
        not_out=not_out.groupby(['batter','season']).sum(numeric_only=True)['not_out']

        match_inning_over_by_ball=match_inning_over_by_ball.groupby(by=['season','batter']).sum(numeric_only=True)
        batsman_stats=match_inning_over_by_ball.sort_values(by='runs_total',ascending=False).drop(['match_id','innings_nbr','over_count'],axis=1).reset_index()
        # batsman_stats[0:10]

        def faced_balls(x):
            return x[0]-x[1]

        batsman_stats['faced_balls']=batsman_stats[['balls','wide_count']].apply(faced_balls,axis=1)

        def strike_rate(x):

            return round(((x[0]/(x[1]))*100),2)

        batsman_stats['bat_SR%']=batsman_stats[['runs_batter','faced_balls','wicket']].apply(strike_rate,axis=1)


        def bat_avg(x):
            if x[1]>1:
                return round((x[0]/x[1]),2)
            else:
                return None
        batsman_stats['bat_avg']=batsman_stats[['runs_total','wicket']].apply(bat_avg,axis=1)


        no_of_innings=match_inning_over_by_ball.groupby(['match_id','batter','season']).sum(numeric_only=True).reset_index()
        no_of_innings['innings_count']=no_of_innings['match_id'].apply(lambda x:1)
        no_of_innings=pd.DataFrame(no_of_innings.groupby(['batter','season']).sum(numeric_only=True)['innings_count'])
        # no_of_innings[0:10]

        batsman_stats=pd.merge(batsman_stats,highest_score.reset_index(),on=['batter','season'])
        batsman_stats=pd.merge(batsman_stats,not_out.reset_index(),on=['batter','season'])
        batsman_stats=pd.merge(batsman_stats,duck.reset_index(),on=['batter','season'])
        batsman_stats=pd.merge(batsman_stats,set_runs.reset_index(),on=['batter','season'])
        batsman_stats=pd.merge(batsman_stats,no_of_innings.reset_index(),on=['batter','season'])

        # %%
        batsman_stats

        # %%
        bg=batsman_stats.groupby('batter').sum(numeric_only=True).reset_index()
        bg

        # %%
        try:
            duckdb.sql("CREATE TABLE batsman_stats AS SELECT * FROM batsman_stats")   # re run  this , it might fail for 1st run
        except:
            duckdb.sql("CREATE TABLE batsman_stats AS SELECT * FROM batsman_stats")  # this will fail if table already exists hence finally none
        finally:
            None

        # %%
        batsman_stats=duckdb.sql("select * from batsman_stats").df()

        # %%
        batsman_stats=batsman_stats[batsman_stats['season']=='2023']

        # %%
        data = batsman_stats.groupby('batter').sum(numeric_only=True).reset_index().sort_values(by='6s', ascending=False)
        data = data[data['6s'] > 5]

        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['batter'], y=data['6s'],
                            marker=dict(color='red'),
                            hovertemplate='%{y}'))

        fig.update_layout(
            title='Most 6s (sixes) in IPL 2023',
            xaxis_title='Batter',
            yaxis_title='6s (sixes)',
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['batter'][:10],
                range=[-0.5, 15]
            )
        )

        pio.write_html(fig, file='template_2023/source/most_6s.html', auto_open=False, full_html=False, include_plotlyjs='cdn')

        # Show the graph
        #pyo.iplot(fig)


        # %%
        data=batsman_stats.groupby('batter').sum(numeric_only=True).reset_index().sort_values(by='4s',ascending=False)
        data=data[data['4s']>10]
        # data=data[data['4s']>200]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['batter'], y=data['4s'], 
                            marker=dict(color='teal'), # specify the desired color here
                            hovertemplate='%{y}'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Most 4s (fours) in IPL History',
                        title='Most 4s (fours) in IPL 2023',
                        xaxis_title='Batter',
                        yaxis_title='4s (fours)',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['batter'][:10],
                range=[-0.5, 15])
        )

        # pyo.plot(fig, filename='most_4s.html')
        pio.write_html(fig, file='template_2023/source/most_4s.html', auto_open=False,full_html=False,include_plotlyjs='cdn')

        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=batsman_stats.groupby('batter').sum(numeric_only=True).reset_index().sort_values(by='bat_SR%',ascending=True)
        # data=data[data['innings_count']>100]
        data=data[data['balls']>10]
        fig = go.Figure()
        fig.add_trace(go.Bar(y=data['batter'], x=data['bat_SR%'], orientation='h',
                            marker=dict(color='blue'), # specify the desired color here
                            hovertemplate='%{x}'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Batting Strike% in IPL History',
            title='Batting Strike% in IPL 2023',
                        yaxis_title='Batter',
                        xaxis_title='Batting Strike%',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
            yaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['batter'][:10],
                range=[len(data)-10, len(data)])

        )
        # pyo.plot(fig, filename='bat_sr.html')
        pio.write_html(fig, file='template_2023/source/bat_sr.html', auto_open=False,full_html=False,include_plotlyjs='cdn')

        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=batsman_stats.groupby('batter').sum(numeric_only=True).reset_index().sort_values(by='runs_batter',ascending=True)
        # data=data[data['runs.total']>2000]
        data=data[data['runs_total']>100]
        fig = go.Figure()
        fig.add_trace(go.Bar(y=data['batter'], x=data['runs_batter'], orientation='h',
                            marker=dict(color='purple'), # specify the desired color here
                            hovertemplate='%{x}'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Most Runs in IPL History',
                        title='Most Runs in IPL 2023',
                        yaxis_title='Batter',
                        xaxis_title='Most Runs',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
            yaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['batter'][:10],
                range=[len(data)-10, len(data)])
        )
        # pyo.plot(fig, filename='most_runs.html')
        pio.write_html(fig, file='template_2023/source/most_runs.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        # Sort and filter the data
        data = batsman_stats.groupby('batter').sum(numeric_only=True).reset_index().sort_values(by='6s', ascending=False)
        data = data[data['6s'] > 5]

        # Create frames for each column value
        frames = []
        for col in ['4s', '6s']:
            frame = go.Frame(data=[go.Bar(x=data['batter'], y=data[col],
                                        marker=dict(color='red'),
        #                                  hovertemplate='<b>4s</b>: %{y}<extra></extra>',
                                        hovertemplate='<b>%{x}</b> : %{y}<extra></extra>')])
            frames.append(frame)

        # Create figure and add traces
        fig = go.Figure(
            data=[go.Bar(x=data['batter'], y=data['6s'],
                        marker=dict(color='red'),
                        hovertemplate='%{y}')],
            frames=frames
        )

        # Update layout
        fig.update_layout(
            title='Most 6s (sixes) and 4s (fours) in IPL 2023',
            xaxis_title='Batter',
            yaxis_title='Count',
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict({'categoryorder': 'total descending'},
                    type='category',
                    dtick=1,
                    tickmode='linear',
                    tick0=0,
                    tickvals=data['batter'][:10],
                    range=[-0.5, 15]
                    ),
            updatemenus=[
                {
                    "buttons": [
                        {
                            "args": [None, {"frame": {"duration": 2000, "redraw": False},
                                            "fromcurrent": True, "transition": {"duration": 300, "easing": "quadratic-in-out"}}],
                            "label": "4s",
                            "method": "animate"
                        },

                        {
                            "args": [[None], {"frame": {"duration": 0, "redraw": False}, "mode": "immediate",
                                            "transition": {"duration": 0}}],
                            "label": "Pause",
                            "method": "animate"
                        }
                    ],
                    "direction": "left",
                    "pad": {"r": 10, "t": 10},  # Adjust the padding here
                    "showactive": False,
                    "type": "buttons",
                    "x":1,
                    "xanchor": "left",
                    "y": 1,
                    "yanchor": "top"
                }
            ]
        )

        # Add frames to figure
        fig.frames = frames

        # Update batters names on button click
        fig.update_layout(
            updatemenus=[
                dict(
                    buttons=[
                        dict(
                            args=[
                                {"x": [data['batter']],
                                "y": [data['4s']]},
                                {"xaxis.title.text": "Batter", "yaxis.title.text": "4s (fours)"}
                            ],
                            label="4s",
                            method="update"
                        ),
                        dict(
                            args=[
                                {"x": [data['batter']],
                                "y": [data['6s']]},
                                {"xaxis.title.text": "Batter", "yaxis.title.text": "6s (sixes)"}
                            ],
                            label="6s",
                            method="update"
                        )
                    ],
                    direction="down",
                    showactive=True,
                    x=0.9,  # Adjust the x-position here
                    y=0.95,  # Adjust the y-position here
                    xanchor="left",
                    yanchor="top"
                )
            ]
        )

        pio.write_html(fig, file='template_2023/source/most_6s_4s.html', auto_open=False, full_html=False, include_plotlyjs='cdn')

        # Show the graph
        #pyo.iplot(fig)


        # %%
        def run_runs(x):return x[2]-( 4*x[0] + 6*x[1])
        batsman_stats['run_runs']=batsman_stats[['4s','6s','runs_batter']].apply(run_runs,axis=1)
        def boundary_runs(x):return ( 4*x[0] + 6*x[1])
        batsman_stats['boundary_runs']=batsman_stats[['4s','6s','runs_batter']].apply(boundary_runs,axis=1)

        data = batsman_stats.groupby('batter').sum(numeric_only=True).reset_index().sort_values(by='runs_batter', ascending=False)
        data = data[data['runs_batter'] > 100]

        fig = go.Figure()

        # Bar chart
        fig.add_trace(go.Bar(
            x=data['batter'],
            y=data['runs_batter'],
            marker=dict(color='purple'),
            hovertemplate='<b>%{x}</b><br>Batter Runs: %{y}<extra></extra>',
        #     hovertemplate='%{y}',
        # #     name='Runs'+"   ",
        # #     orientation='v',
            name='Batter Runs'+"   "
        ))

        # Line graph
        fig.add_trace(go.Scatter(
            x=data['batter'],
            y=data['bat_SR%'],
            mode='lines+markers',
            line=dict(color='red'),
            yaxis='y1',
            hovertemplate='<b>%{x}</b><br>Strike Rate: %{y}<extra></extra>',
        #     hovertemplate='%{y}',
            name='Strike Rate'+"   "
        ))

        fig.add_trace(go.Scatter(
            x=data['batter'],
            y=data['run_runs'],
            mode='lines+markers',
            line=dict(color='yellow'),
            yaxis='y1',
            hovertemplate='<b>%{x}</b><br>Ran Runs: %{y}<extra></extra>',
        #     hovertemplate='%{y}',
            name='Ran Runs'+"   "
        ))

        fig.add_trace(go.Scatter(
            x=data['batter'],
            y=data['boundary_runs'],
            mode='lines+markers',
            line=dict(color='white'),
            yaxis='y1',
            hovertemplate='<b>%{x}</b><br>Boundary Runs: %{y}<extra></extra>',
        #     hovertemplate='%{y}',
            name='Only Boundary'+"   ",
        #     font_color='white',
        #     bgcolor='black'
        ))

        fig.add_trace(go.Scatter(
            x=data['batter'],
            y=data['HS'],
            mode='lines+markers',
            line=dict(color='orange'),
            yaxis='y1',
            hovertemplate='<b>%{x}</b><br>Highest Score: %{y}<extra></extra>',
        #     hovertemplate='%{y}',
            name='High Score'+"   "
        ))

        # Customize the graph layout
        fig.update_layout(
            title='Most Runs in IPL 2023',
            yaxis_title='Runs',
            xaxis_title='Batsman',
            yaxis2=dict(
                title='Runs',
                overlaying='y',
                side='left',
                            ),
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['batter'][:10],
        #         range=[len(data)-10, len(data)]
                range=[-0.5, 15]
            )
            ,legend=dict(x=0.5,
                y=1,
                bgcolor='rgba(255, 255, 255, 0.5)',
                bordercolor='rgba(0, 0, 0, 0.5)',
                borderwidth=1,
                orientation='h',  # Horizontal orientation of legends
                font=dict(
                    family='Helvetica',
                    size=10,
                    color='black',
                ),
                xanchor='center',  # Legends centered horizontally
                yanchor='bottom',
            )
            ,hovermode='closest'
        #     ,hoverlabel=dict(
        #         bgcolor="black",
        #         font_size=16,
        # #         font_family="Rockwell"
        #     )
            
        )

        # Save the graph as HTML
        pio.write_html(fig, file='template_2023/source/most_runs.html', auto_open=False, full_html=False, include_plotlyjs='cdn')
        #pyo.iplot(fig)

        # %%
        data=batsman_stats.groupby('batter').sum(numeric_only=True).reset_index().sort_values(by='50s',ascending=False)
        # data=data[data['runs.total']>2000]
        data=data[data['50s']>0]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['batter'], y=data['50s'], 
                            marker=dict(color='olive'), # specify the desired color here
                            hovertemplate='<b>%{x}<b>:%{y}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Most Runs in IPL History',
                        title='50s in IPL 2023',
                        xaxis_title='Batter',
                        yaxis_title='50s',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['batter'][:10],
                range=[-0.5, 15])
        )
        # pyo.plot(fig, filename='most_runs.html')
        pio.write_html(fig, file='template_2023/source/_50s.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=batsman_stats.groupby('batter').sum(numeric_only=True).reset_index().sort_values(by='100s',ascending=False)
        # data=data[data['runs.total']>2000]
        data=data[data['100s']>0]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['batter'], y=data['100s'], 
                            marker=dict(color='gold'), # specify the desired color here
                            hovertemplate='<b>%{x}</b>:%{y}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Most Runs in IPL History',
                        title='100s in IPL 2023',
                        xaxis_title='Batter',
                        yaxis_title='100s',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['batter'][:10],
                range=[-0.5, 8.5])
        )
        # pyo.plot(fig, filename='most_runs.html')
        pio.write_html(fig, file='template_2023/source/_100s.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        match_inning_over_by_ball=match_inning_over_by_ballz.copy()
        match_inning_over_by_ball=match_inning_over_by_ball[match_inning_over_by_ball['season']=='2023']
        ducker=match_inning_over_by_ball.groupby(['batter','match_id']).sum(numeric_only=True)
        ducker=ducker[(ducker['runs_total']==0) & (ducker['wicket']==1)].groupby('batter').sum(numeric_only=True)
        ducker=ducker.reset_index()

        # %%
        data=ducker.sort_values(by='wicket',ascending=True)
        # data=data[data['runs.total']>2000]
        data=data[data['wicket']>1]
        fig = go.Figure()
        fig.add_trace(go.Bar(y=data['batter'], x=data['wicket'], orientation='h',
                            marker=dict(color='peru'), # specify the desired color here
                            hovertemplate='<b>%{y}</b>:%{x}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Most Runs in IPL History',
                        title='Duck Dismissals in IPL 2023',
                        yaxis_title='Batter',
                        xaxis_title='Ducks',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        yaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['batter'][:10],
                range=[len(data)-10, len(data)]))
        # pyo.plot(fig, filename='most_runs.html')
        pio.write_html(fig, file='template_2023/source/duck.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        # match_inning_over_by_ball=match_inning_over_by_ball[match_inning_over_by_ball['season']=='2023']

        # Group the data by match or innings and batsman combination
        partnerships = match_inning_over_by_ball.groupby(['match_id', 'batter', 'non_striker'])

        # Calculate total partnership runs and average partnership for each combination
        partnership_data = partnerships['runs_total'].sum(numeric_only=True)
        partnership_average = partnerships['runs_total'].mean()

        # Create a new DataFrame to store partnership analysis
        partnership_analysis = pd.DataFrame({
            'Batsman': partnership_data.index.get_level_values('batter'),
            'Non-Striker': partnership_data.index.get_level_values('non_striker'),
            'Total Runs': partnership_data.values,
            'Average Partnership': partnership_average.values,
            "Against" : partnerships['team_bowl'],
        })

        # Print the partnership analysis
        partnership_analysis=partnership_analysis.sort_values(by='Total Runs',ascending= False)
        partnership_analysis['Partners']=partnership_analysis['Batsman']+ " & "+partnership_analysis['Non-Striker']
        partnership_analysis


        # %%
        data=partnership_analysis
        # data=data[data['runs.total']>2000]
        data=data[data['Total Runs']>50]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['Partners'], y=data['Total Runs'], 
                            marker=dict(color='darkmagenta'), # specify the desired color here
                            hovertemplate='<b>%{x}</b><br>Partnership : %{y}<extra></extra>'))



        # Customize the graph layout
        fig.update_layout(
        #     title='Most Runs in IPL History',
                        title='Best Partnership in a match in IPL 2023',
                        xaxis_title='Partners',
                        yaxis_title='Runs',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        #     xaxis={'categoryorder': 'total descending'},
        xaxis=dict(categoryorder='total descending',
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['Partners'][:10],
                range=[-0.5, 20])
        )
        # pyo.plot(fig, filename='most_runs.html')
        pio.write_html(fig, file='template_2023/source/partnership.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        match_inning_over_by_ball=match_inning_over_by_ballz.copy()

        match_inning_over_by_ball['dates']=match_inning_over_by_ball['dates'].apply(lambda x:str(x))

        wicket_count=pd.DataFrame(match_inning_over_by_ball.groupby(['bowler','match_id','season']).sum(numeric_only=True))
        wicket_count['5w']=wicket_count['bowler_wicket'].apply(lambda x: 1 if x>4 and x<10 else 0)
        wicket_count['4w']=wicket_count['bowler_wicket'].apply(lambda x: 1 if x>3 and x<5 else 0)
        wicket_count=wicket_count.groupby(['bowler','season']).sum(numeric_only=True)[['5w','4w']].reset_index()
        # wicket_count[0:10]

        no_of_innings_bow=match_inning_over_by_ball.groupby(['match_id','bowler','season']).sum(numeric_only=True).reset_index()
        no_of_innings_bow['innings_count']=no_of_innings_bow['match_id'].apply(lambda x:1)
        no_of_innings_bow=pd.DataFrame(no_of_innings_bow.groupby(['bowler','season']).sum(numeric_only=True)['innings_count']).reset_index()
        # no_of_innings_bow[0:10]
        match_inning_over_by_ball=match_inning_over_by_ball.groupby(by=['season','bowler']).sum(numeric_only=True)
        bowling_stats=match_inning_over_by_ball.sort_values(by='wicket',ascending=False).drop(['match_id','innings_nbr','over_count'],axis=1).reset_index()
        # bowling_stats[0:10]

        def runs_conceded(x):
            return x[0]-x[1]-x[2]
        bowling_stats['runs_conceded']=bowling_stats[['runs_total','extras_byes','extras_legbyes','extras_penalty']].apply(runs_conceded,axis=1)

        def correct_balls(x):
            return x[0]-x[1]-x[2]
        bowling_stats['correct_balls']=bowling_stats[['balls','wide_count','extras_noballs']].apply(correct_balls,axis=1)

        def ER(x):
                return round((x[0]/(x[1]/6)),2)
        bowling_stats['ER']=bowling_stats[['runs_conceded','correct_balls']].apply(ER,axis=1)

        def bowl_strike_rate(x):
            if x[1]>0:
                return round((x[0]/x[1]),2)
            else:
                return None
        bowling_stats['bowl_SR%']=bowling_stats[['correct_balls','bowler_wicket']].apply(bowl_strike_rate,axis=1)

        def bowl_avg(x):
            if x[1]>0:
                return round((x[0]/x[1]),2)
            else:
                return None
        bowling_stats['bowl_avg']=bowling_stats[['runs_conceded','bowler_wicket']].apply(bowl_avg,axis=1)
        # bowler_maiden_count=bowler_maiden_count.reset_index()[['bowler','maiden']]
        # bowling_stats=pd.merge(bowling_stats,bowler_maiden_count,on=['bowler','season'])
        bowling_stats=pd.merge(bowling_stats,no_of_innings_bow,on=['bowler','season'])
        bowling_stats=pd.merge(bowling_stats,wicket_count,on=['bowler','season'])
        # bowler_maiden_count[0:10]


        # %%
        try:
            duckdb.sql("CREATE TABLE bowling_stats AS SELECT * FROM bowling_stats")   # re run  this , it might fail for 1st run
        except:
            duckdb.sql("CREATE TABLE bowling_stats AS SELECT * FROM bowling_stats")   # this will fail if table already exists hence finally none
        finally:
            None

        # %%
        bowling_stats=duckdb.sql("select * from bowling_stats").df()

        # %%
        bowling_stats= bowling_stats[bowling_stats['season']=='2023']

        # %%
        data=bowling_stats.sort_values(by='bowler_wicket',ascending=False)
        # data=data[data['wicket']>100]
        data=data[data['bowler_wicket']>5]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['bowler'], y=data['bowler_wicket'], 
                            marker=dict(color='orange'), # specify the desired color here
                            hovertemplate='%{y}'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Most Wickets in IPL History',
                        title='Most Wickets in IPL 2023',
                        xaxis_title='Bowler',
                        yaxis_title='Wickets',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(categoryorder='total descending',
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 15])
        )
        # pyo.plot(fig, filename='most_wickets.html')
        pio.write_html(fig, file='template_2023/source/most_wickets.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=bowling_stats.sort_values(by='balls',ascending=False)
        # data=data[data['wicket']>100]
        data=data[data['balls']>30]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['bowler'], y=data['balls'], 
                            marker=dict(color='lightblue'), # specify the desired color here
                            hovertemplate='<b>%{x}</b><br>Balls : %{y}<extra></extra>',
                            name='Balls bowled'+"   "))

        fig.add_trace(go.Scatter(
            x=data['bowler'],
            y=data['wide_count'],
            mode='lines+markers',
            line=dict(color='red'),
            yaxis='y2',
            hovertemplate='<b>%{x}</b><br>Wides: %{y}<extra></extra>',
            name='Wides'+"   "
        ))

        fig.add_trace(go.Scatter(
            x=data['bowler'],
            y=data['extras_noballs'],
            mode='lines+markers',
            line=dict(color='black'),
            yaxis='y2',
            hovertemplate='<b>%{x}</b><br>No Balls: %{y}<extra></extra>',
            name='No Balls'+"   "
        ))

        # Customize the graph layout


        # Customize the graph layout
        fig.update_layout(
        #     title='Most Wickets in IPL History',
                        title='No of Balls, Wides & NO Balls in IPL 2023',
                        xaxis_title='Bowler',
                        yaxis_title='Balls_bowled',  
                yaxis2=dict(
                title='Wides/No Balls',
                overlaying='y',
                side='right',
            ),
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 15]),
            
            legend=dict(x=0.5,
                y=1,
                bgcolor='rgba(255, 255, 255, 0.5)',
                bordercolor='rgba(0, 0, 0, 0.5)',
                borderwidth=1,
                orientation='h',  # Horizontal orientation of legends
                font=dict(
                    family='Helvetica',
                    size=10,
                    color='black',
                ),
                xanchor='center',  # Legends centered horizontally
                yanchor='bottom',
            )
        #     ,width=1200
        )
        # pyo.plot(fig, filename='most_wickets.html')
        pio.write_html(fig, file='template_2023/source/BWN.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        import plotly.graph_objects as go
        import plotly.io as pio

        data = bowling_stats.sort_values(by='bowler_wicket', ascending=False)
        data = data[data['bowler_wicket'] > 5]

        fig = go.Figure()

        # Bar chart
        fig.add_trace(go.Bar(
            x=data['bowler'],
            y=data['bowler_wicket'],
            marker=dict(color='orange'),
            hovertemplate='<b>%{x}</b><br>Wickets: %{y}<extra></extra>',
            name='Wickets'+"   "
        ))

        # Line graph
        fig.add_trace(go.Scatter(
            x=data['bowler'],
            y=data['ER'],
            mode='lines+markers',
            line=dict(color='blue'),
            yaxis='y2',
            hovertemplate='<b>%{x}</b><br>Economy: %{y}<extra></extra>',
            name='Economy'+"   "
        ))

        # Customize the graph layout
        fig.update_layout(
            title='Most Wickets in IPL 2023',
            xaxis_title='Bowler',
            yaxis_title='Wickets',
            yaxis2=dict(
                title='Economy Rating',
                overlaying='y',
                side='right',
            ),
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict(
        #         categoryorder='total descending',
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 15]
            ),legend=dict(x=0.9,
                y=0.9,
                bgcolor='rgba(255, 255, 255, 0.5)',
                bordercolor='rgba(0, 0, 0, 0.5)',
                borderwidth=1,
                orientation='v',  # Horizontal orientation of legends
                font=dict(
                    family='Helvetica',
                    size=10,
                    color='black',
                ),
                xanchor='center',  # Legends centered horizontally
                yanchor='bottom',
            )
        )

        # Save the graph as HTML
        pio.write_html(fig, file='template_2023/source/most_wickets.html', auto_open=False, full_html=False, include_plotlyjs='cdn')
        #pyo.iplot(fig)

        # %%

        wicket_df=pd.DataFrame(batsman_stats.sum(numeric_only=True)).transpose()[[ 'wickets_kind_bowled', 'wickets_kind_caught',
                                                    'wickets_kind_caught_and_bowled', 'wickets_kind_hit_wicket',
                                                    'wickets_kind_lbw', 'wickets_kind_retired_hurt',
                                                    'wickets_kind_retired_out', 'wickets_kind_run_out',
                                                    'wickets_kind_stumped']]

        wicket_df=wicket_df.transpose().reset_index()
        wicket_df.columns=['type','count']
        wicket_df['type']=wicket_df['type'].apply(lambda x:x.split("_")[2])
        wicket_df
        import plotly.express as px
        df = wicket_df
        fig = px.pie(df, values='count', names='type', title='Percentage of Dissmissal types')
        pio.write_html(fig, file='template_2023/source/wkt_perc.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        #fig.show()


        # %%

        data = bowling_stats.sort_values(by='bowler_wicket', ascending=False)
        data = data[data['bowler_wicket'] > 5]

        fig = go.Figure()

        # Bar chart
        fig.add_trace(go.Bar(
            x=data['bowler'],
            y=data['bowler_wicket'],
            marker=dict(color='orange'),
            hovertemplate='<b>%{x}</b><br>Wickets: %{y}<extra></extra>',
            name='Wickets'+"   "
        ))

        # Line graph
        fig.add_trace(go.Scatter(
            x=data['bowler'],
            y=data['ER'],
            mode='lines+markers',
            line=dict(color='blue'),
            yaxis='y2',
            hovertemplate='<b>%{x}</b><br>Economy: %{y}<extra></extra>',
            name='Economy'+"   "
        ))

        # Customize the graph layout
        fig.update_layout(
            title='Most Wickets in IPL 2023',
            xaxis_title='Bowler',
            yaxis_title='Wickets',
            yaxis2=dict(
                title='Economy Rating',
                overlaying='y',
                side='right',
            ),
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict(
        #         categoryorder='total descending',
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 15]
            ),legend=dict(x=0.9,
                y=0.9,
                bgcolor='rgba(255, 255, 255, 0.5)',
                bordercolor='rgba(0, 0, 0, 0.5)',
                borderwidth=1,
                orientation='v',  # Horizontal orientation of legends
                font=dict(
                    family='Helvetica',
                    size=10,
                    color='black',
                ),
                xanchor='center',  # Legends centered horizontally
                yanchor='bottom',
            )
        )

        # Save the graph as HTML
        pio.write_html(fig, file='template_2023/source/most_wickets.html', auto_open=False, full_html=False, include_plotlyjs='cdn')
        #pyo.iplot(fig)

        # %%
        match_inning_over_by_ball=match_inning_over_by_ballz.copy()

        maiden=match_inning_over_by_ball.copy()

        maiden['innings_nbr']=maiden['innings_nbr'].apply(lambda x:str(x))
        maiden['match_id']=maiden['match_id'].apply(lambda x:str(x))
        maiden['over_count']=maiden['over_count'].apply(lambda x:str(x))

        # maiden=maiden[maiden['season']=='2023']



        maiden['unique_over']="over "+maiden['match_id']+maiden['innings_nbr']+maiden['over_count']

        maiden=maiden.groupby(['unique_over','bowler']).sum(numeric_only=True).reset_index()
        def maidener(x):
            if x[0]==0 and x[1]==6:
                return 1
            else :
                return 0
        maiden['maiden']=maiden[['runs_total','balls']].apply(maidener,axis=1)

        # maiden['balls'].value_counts()

        maiden=pd.DataFrame(maiden.groupby('bowler').sum(numeric_only=True)['maiden']).reset_index()
        maiden.columns=['bowler','maiden']
        # maiden['maiden'].value_counts()

        # %%
        data=maiden.sort_values(by='maiden',ascending=False)
        # data=data[data['maiden']>1]
        # data=maiden
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['bowler'], y=data['maiden'], 
                            marker=dict(color='green'), # specify the desired color here
                            hovertemplate='<b>%{x}</b><br>Maidens: %{y}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Most Maidens in IPL History',
            title='Most Maidens in IPL 2023',
                        xaxis_title='Bowler',
                        yaxis_title='Maidens',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 8.5])
        )

        # pyo.plot(fig, filename='most_maidens.html')
        pio.write_html(fig, file='template_2023/source/most_maidens.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=bowling_stats.sort_values(by='ER',ascending=False)
        # data=data[data['ER']>]
        data=data[(data['balls']>30) & (data['ER']<8)]
        data=data.groupby('bowler').mean(numeric_only=True).reset_index()
        fig = go.Figure()
        fig.add_trace(go.Bar(y=data['bowler'], x=data['ER'], orientation='h',
                            marker=dict(color='magenta'), # specify the desired color here
                            hovertemplate='%{x}'))

        # Customize the graph layout
        fig.update_layout(
        #     title="Bowler's Economy in IPL History",
                        title="Bowler's Economy in IPL 2023",
                        yaxis_title='Bowler',
                        xaxis_title='Economy Rate',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        yaxis=dict(categoryorder='total descending',
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10][::-1],
                range=[len(data)-10.5,len(data)])
        )

        # pyo.plot(fig, filename='ER.html')
        pio.write_html(fig, file='template_2023/source/ER.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=bowling_stats.sort_values(by='bowl_SR%',ascending=False)
        # data=data[data['bowl_SR%']>500]
        data=data[data['bowl_SR%']<15]
        data=data.groupby('bowler').mean(numeric_only=True).reset_index()
        fig = go.Figure()
        fig.add_trace(go.Bar(y=data['bowler'], x=data['bowl_SR%'],orientation='h', 
                            marker=dict(color='#bcbd22'), # specify the desired color here
                            hovertemplate='%{x}'))

        # Customize the graph layout
        fig.update_layout(
        #     title="Bowler's strike rate% in IPL History",
                        title="Bowler's strike rate% in IPL 2023",
                        yaxis_title='Bowler',
                        xaxis_title='Strike rate%',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        yaxis=dict(categoryorder='total descending',
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5,10.5])
        )

        # pyo.plot(fig, filename='ER.html')
        pio.write_html(fig, file='template_2023/source/bowl_sr.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=bowling_stats.sort_values(by='4w',ascending=False)
        # data=data[data['bowl_SR%']>500]
        data=data[data['4w']>0]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['bowler'], y=data['4w'], 
                            marker=dict(color='dodgerblue'), # specify the desired color here
                            hovertemplate='<b>%{x}</b><br>4w: %{y}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
        #     title="Bowler's strike rate% in IPL History",
                        title="4 Wickets in an inning in IPL 2023",
                        xaxis_title='Bowler',
                        yaxis_title='4 wickets in an inning',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 15])
        )

        # pyo.plot(fig, filename='ER.html')
        pio.write_html(fig, file='template_2023/source/_4w.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=bowling_stats.sort_values(by='5w',ascending=False)
        # data=data[data['bowl_SR%']>500]
        data=data[data['5w']>0]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['bowler'], y=data['5w'], 
                            marker=dict(color='dimgray'), # specify the desired color here
                            hovertemplate='<b>%{x}</b><br>5w: %{y}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
        #     title="Bowler's strike rate% in IPL History",
                        title="5 Wickets in an inning in IPL 2023",
                        xaxis_title='Bowler',
                        yaxis_title='5 wickets in an inning',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 3.5])
        )

        # pyo.plot(fig, filename='ER.html')
        pio.write_html(fig, file='template_2023/source/_5w.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=bowling_stats.sort_values(by='runs_conceded',ascending=False)
        # data=data[data['bowl_SR%']>500]
        data=data[data['runs_conceded']>200]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['bowler'], y=data['runs_conceded'], 
                            marker=dict(color='darksalmon'), # specify the desired color here
                            hovertemplate='%{y}'))

        # Customize the graph layout
        fig.update_layout(
        #     title="Bowler's strike rate% in IPL History",
                        title="Total Runs conceded by a bowler in IPL 2023",
                        xaxis_title='Bowler',
                        yaxis_title='Runs Conceded',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 15])
        )

        # pyo.plot(fig, filename='ER.html')
        pio.write_html(fig, file='template_2023/source/RC.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        data=bowling_stats.sort_values(by='bowl_SR%',ascending=True)
        # data=data[data['bowl_SR%']>500]
        data=data[data['bowl_SR%']<15]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['bowler'], y=data['bowl_SR%'], 
                            marker=dict(color='#bcbd22'), # specify the desired color here
                            hovertemplate='<b>%{x}</b><br>Bowl Strike Rate: %{y}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
        #     title="Bowler's strike rate% in IPL History",
                        title="Bowler's strike rate% in IPL",
                        xaxis_title='Bowler',
                        yaxis_title='Strike rate%',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['bowler'][:10],
                range=[-0.5, 15])
        )

        # pyo.plot(fig, filename='ER.html')
        pio.write_html(fig, file='template_2023/source/bowl_sr.html', auto_open=False,full_html=False,include_plotlyjs='cdn')
        # Show the graph
        #pyo.iplot(fig)

        # %%
        catch_data=match_inning_over_by_ball=match_inning_over_by_ball[match_inning_over_by_ball['season']=='2023']
        catch_data=match_inning_over_by_ball
        catches=pd.DataFrame(catch_data['wickets_fielder_name'].value_counts()).reset_index()
        catches.columns=['fielder','count']

        # %%
        data=catches.sort_values(by='count',ascending=False)
        # data=data[data['6s']>80]
        data=data[data['count']>4]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=data['fielder'], y=data['count'], 
                            marker=dict(color='red'), # specify the desired color here
                            hovertemplate='<b>%{x}</b><br>Catches: %{y}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(

            title='Most catches in IPL 2023',
            
                        xaxis_title='Caught by',
                        yaxis_title='catch count',                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
            xaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['fielder'][:10],
                range=[-0.5, 15])
        )

        # pyo.plot(fig, filename='most_6s.html')
        pio.write_html(fig, file='template_2023/source/catches.html', auto_open=False,full_html=False,include_plotlyjs='cdn')

        # Show the graph
        #pyo.iplot(fig)

        # %%
        match_inning_over_by_ball=match_inning_over_by_ballz.copy()
        match_inning_over_by_ball=match_inning_over_by_ball[match_inning_over_by_ball['season']=='2023']

        match_inning_over_by_ball['result']=match_inning_over_by_ball['result'].fillna('won')

        def points_1(x):
            if x[0]=='no result' or x[0]=='tie':
                return x[2]
            else:
                return x[1]
        match_inning_over_by_ball['winner']=match_inning_over_by_ball[['result','winner','team']].apply(points_1,axis=1)

        # match_inning_over_by_ball.groupby(['result','team','team_bowl','winner','match_id']).sum(numeric_only=True)

        team_points=match_inning_over_by_ball.groupby(['team','team_bowl','winner','match_id','result']).sum(numeric_only=True).reset_index()[['team','team_bowl','winner','match_id','result']]
        # team_points['points']=team_points['']

        team_points['points']=team_points['result'].apply(lambda x:1 if x=='won' else 0.5)

        team_points=team_points.groupby('winner').sum(numeric_only=True).drop('match_id',axis=1)

        # %%
        data=team_points.reset_index().sort_values(by='points',ascending=True)
        fig = go.Figure()
        fig.add_trace(go.Bar(y=data['winner'], x=data['points'], orientation='h',
                            marker=dict(color='navy'), # specify the desired color here
                            hovertemplate='<b>%{y}</b><br>Win Points: %{x}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
                        title='Points IPL 2023',
                        yaxis_title='Teams',
                        xaxis_title='Points',
        plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',)

        # pyo.plot(fig, filename='most_4s.html')
        pio.write_html(fig, file='template_2023/source/team_points.html', auto_open=False,full_html=False,include_plotlyjs='cdn')

        # Show the graph
        #pyo.iplot(fig)

        # %%
        mom=pd.DataFrame(match_inning_over_by_ball.groupby(['match_id','player_of_match']).sum(numeric_only=True).reset_index()['player_of_match'].value_counts()).reset_index()
        mom.columns=['MOM','Counts']
        # mom

        # %%
        data=mom.sort_values(by='Counts',ascending=True)
        # data=data[data['4s']>10]
        # data=data[data['4s']>200]
        fig = go.Figure()
        fig.add_trace(go.Bar(y=data['MOM'], x=data['Counts'],orientation='h' ,
                            marker=dict(color='fuchsia'), # specify the desired color here
                            hovertemplate='<b>%{y}</b><br>Man of the Match: %{x}<extra></extra>'))

        # Customize the graph layout
        fig.update_layout(
        #     title='Most 4s (fours) in IPL History',
                        title='Man of the match IPL 2023 ',
                        yaxis_title='Man of the macth',
                        xaxis_title='Counts',
            plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        yaxis=dict(
                type='category',
                dtick=1,
                tickmode='linear',
                tick0=0,
                tickvals=data['MOM'][:10],
                range=[len(data)-10,len(data)])
        )

        # pyo.plot(fig, filename='most_4s.html')
        pio.write_html(fig, file='template_2023/source/mom.html', auto_open=False,full_html=False,include_plotlyjs='cdn')

        # Show the graph
        #pyo.iplot(fig)

        # %%
        match_inning_over_by_ball[['match_id','innings_nbr','team','team_bowl']]

        # %%
        team_vs_runs=match_inning_over_by_ball.drop('dates',axis=1)
        team_vs_runs=team_vs_runs.groupby(['match_id','innings_nbr','team','team_bowl']).sum(numeric_only=True).reset_index()[['match_id','innings_nbr','team','team_bowl','runs_total']]

        team_vs_runs=team_vs_runs.groupby(['team','team_bowl']).max().reset_index()[['team','team_bowl','runs_total']]
        team_vs_runs=team_vs_runs.set_index('team')

        team_vs_runs=team_vs_runs.pivot(columns='team_bowl')

        new_columns=[]
        for i in list(team_vs_runs.columns):
            new_columns.append(i[1])

        team_vs_runs.columns=new_columns

        # %%
        # Define the x-axis labels
        x_labels = team_vs_runs.reset_index()['team']

        # Define the trace for each type of wicket
        traces = [
            go.Bar(x=x_labels, y=team_vs_runs['Chennai Super Kings'], name='Chennai Super Kings',),
            go.Bar(x=x_labels, y=team_vs_runs['Delhi Capitals'], name='Delhi Capitals'),
            go.Bar(x=x_labels, y=team_vs_runs['Gujarat Titans'], name='Gujarat Titans'),
            go.Bar(x=x_labels, y=team_vs_runs['Kolkata Knight Riders'], name='Kolkata Knight Riders'),
            go.Bar(x=x_labels, y=team_vs_runs['Lucknow Super Giants'], name='Lucknow Super Giants'),
            go.Bar(x=x_labels, y=team_vs_runs['Mumbai Indians'], name='Mumbai Indians'),
            go.Bar(x=x_labels, y=team_vs_runs['Punjab Kings'], name='Punjab Kings'),
            go.Bar(x=x_labels, y=team_vs_runs['Rajasthan Royals'], name='Rajasthan Royals'),
            go.Bar(x=x_labels, y=team_vs_runs['Royal Challengers Bangalore'], name='Royal Challengers Bangalore'),
            go.Bar(x=x_labels, y=team_vs_runs['Sunrisers Hyderabad'], name='Sunrisers Hyderabad')
        ]

        # Define the layout
        layout = go.Layout(
            title='Max score against each team',
            xaxis=dict(title='Team'),
            yaxis=dict(title='Max Score'),
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)'
        )

        # Modify the texttemplate of each trace
        for trace in traces:
            trace.texttemplate = '<span style="background-color: yellow; font-weight: bold;"> VS %{text}</span>'
            trace.text = [trace.name] * len(x_labels)

        # Create the figure object
        fig = go.Figure(data=traces, layout=layout)

        # Save the graph as HTML
        pio.write_html(fig, file='template_2023/source/team_max_score.html', auto_open=False, full_html=False, include_plotlyjs='cdn')

        # Show the figure
        #fig.show()


        # %%
        toss=match_inning_over_by_ball.groupby(['toss_winner','toss_decision','winner']).sum(numeric_only=True).reset_index()[['toss_winner','toss_decision','winner']]

        def match_winner(x):return 1 if x[1]==x[0] else 0
        toss['match_winner']=toss[['toss_winner','winner']].apply(match_winner,axis=1)

        toss=toss.set_index('toss_winner')

        toss['bat']=toss['toss_decision'].apply(lambda x: 1 if x=='bat' else 0)
        toss['field']=toss['toss_decision'].apply(lambda x: 1 if x=='field' else 0)

        toss.groupby('toss_winner').sum(numeric_only=True).reset_index()


        def win_by(x): return 1 if x[0]==1 and x[1]==1 else 0

        toss['win_by_field']=toss[['match_winner','field']].apply(win_by,axis=1)
        toss['win_by_bat']=toss[['match_winner','bat']].apply(win_by,axis=1)

        # %%
        import plotly.graph_objects as go
        data=toss.groupby('toss_winner').sum(numeric_only=True).reset_index()
        # data=toss

        # Define the x-axis labels
        x_labels = data['toss_winner']

        # Define the trace for each type of wicket
        traces = [
            go.Bar(x=x_labels, y=data['bat'], name='Bat'),
            
            go.Bar(x=x_labels, y=data['win_by_bat'], name='Won by Bat'),
            
            go.Bar(x=x_labels, y=data['field'], name='Field'),
            
            go.Bar(x=x_labels, y=data['win_by_field'], name='Won by Field'),
            
            go.Bar(x=x_labels, y=data['match_winner'], name='Won'),
            
        ]

        # Define the layout
        layout = go.Layout(title="Toss winner's Stats IPL 2023", xaxis=dict(title='Toss winning team'), yaxis=dict(title='Choice Count'),
                        plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)'
        )
        for trace in traces:
            trace.texttemplate = '<span style="background-color: yellow; font-weight: bold;">%{text}</span>'
            trace.text = [trace.name] * len(x_labels)


        # Create the figure object
        fig = go.Figure(data=traces, layout=layout)
        pio.write_html(fig, file='template_2023/source//toss_winner_choice.html', auto_open=False,full_html=False,include_plotlyjs='cdn')

        # Show the figure
        #fig.show()

        # %%
        schedule=pd.read_csv('schedule.csv')
        # schedule

        schedule=schedule.set_index(['Date','Time'])
        # schedule

        winners_time=pd.get_dummies(schedule['Winners']).groupby(by='Time').sum(numeric_only=True).transpose().reset_index()
        winners_time.columns=['Team','Evening','Night']
        winners_time

        # %%
        import plotly.graph_objects as go
        data=winners_time[winners_time['Team']!='No result']
        # Define the x-axis labels
        x_labels = data['Team']

        # Define the trace for each type of wicket
        traces = [
            go.Bar(x=x_labels, y=data['Evening'], name='Evening'),
            go.Bar(x=x_labels, y=data['Night'], name='Night'),
            
        ]

        # Define the layout
        layout = go.Layout(title='Win Counts based on match time', xaxis=dict(title='Team'), yaxis=dict(title='Wins'),                     
                    plot_bgcolor='rgba(0, 0, 0, 0)',    
                    paper_bgcolor='rgba(0, 0, 0, 0)',
        barmode='stack'
                        )

        # Create the figure object
        fig = go.Figure(data=traces, layout=layout)
        pio.write_html(fig, file='template_2023/source/team_win_on_game_time.html', auto_open=False,full_html=False,include_plotlyjs='cdn')

        # Show the figure
        #fig.show()

    else:
        print(f"\nThe charts already exist.")
