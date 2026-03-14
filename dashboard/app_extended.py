import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dash import Dash, dcc, html
import os

# ── LOAD DATA ─────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))



users_df = pd.read_csv(os.path.join(BASE_DIR, 'users.csv'), encoding='utf-16')
txn_df   = pd.read_csv(os.path.join(BASE_DIR, 'transactions.csv'), parse_dates=['timestamp'], encoding='utf-16')

# ── PRECOMPUTE METRICS ────────────────────────────────────────────────────────
TOTAL_USERS    = len(users_df)
TOTAL_TXN      = len(txn_df)
TOTAL_VOLUME   = txn_df['amount'].sum()
TOTAL_INCENTIVE = txn_df['incentive'].sum()
AVG_TXN_AMOUNT = txn_df['amount'].mean()

# Transactions per second bucket (group by second)
txn_df['second'] = txn_df['timestamp'].dt.floor('s')
volume_ts = txn_df.groupby('second').size().reset_index(name='count')
volume_ts['t'] = range(len(volume_ts))

# Amount distribution
amounts = txn_df['amount'].tolist()

# Top senders by volume
sender_vol = (
    txn_df.groupby('sender_id')['amount']
    .sum().reset_index()
    .sort_values('amount', ascending=False)
    .head(8)
)
sender_vol['name'] = sender_vol['sender_id'].apply(lambda i: users_df[users_df['id'] == i]['name'].values[0] if i in users_df['id'].values else f'User_{i}')

# User final balances sorted
users_sorted = users_df.sort_values('balance', ascending=False)

# Incentive vs amount
incentive_data = txn_df[txn_df['incentive'] > 0][['amount', 'incentive']]

# Txn volume by user (received)
recv_vol = (
    txn_df.groupby('recipient_id').size().reset_index(name='count')
    .sort_values('count', ascending=False).head(8)
)
recv_vol['name'] = recv_vol['recipient_id'].apply(lambda i: users_df[users_df['id'] == i]['name'].values[0] if i in users_df['id'].values else f'User_{i}')

# ── THEME ─────────────────────────────────────────────────────────────────────
GOLD        = '#C9A84C'
GOLD_BRIGHT = '#F0C040'
GOLD_DIM    = '#7A6030'
BG          = '#080A0E'
BG2         = '#0E1118'
SURFACE     = '#1A1F2E'
TEXT        = '#E8E0CC'
TEXT_DIM    = '#8A8070'
GREEN       = '#4CAF82'
RED         = '#CF6679'
BLUE        = '#2A6496'
PURPLE      = '#7C5CBF'
BORDER      = 'rgba(201,168,76,0.15)'

PLOT_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='JetBrains Mono, monospace', color=TEXT_DIM, size=10),
    margin=dict(t=20, r=20, b=40, l=55),
    xaxis=dict(gridcolor='rgba(201,168,76,0.08)', zerolinecolor='rgba(201,168,76,0.15)'),
    yaxis=dict(gridcolor='rgba(201,168,76,0.08)', zerolinecolor='rgba(201,168,76,0.15)'),
)

# ── CHARTS ────────────────────────────────────────────────────────────────────
def fig_volume():
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=volume_ts['t'],
        y=volume_ts['count'],
        mode='lines+markers',
        line=dict(color=GOLD, width=2, shape='spline'),
        marker=dict(color=GOLD_BRIGHT, size=6),
        fill='tozeroy',
        fillcolor='rgba(201,168,76,0.07)',
        hovertemplate='Batch %{x}: %{y} txns<extra></extra>',
    ))
    fig.update_layout(**PLOT_LAYOUT, height=260,
        yaxis_title='Transactions', xaxis_title='Kafka Batch')
    return fig


def fig_balances():
    colors = [GOLD if b > 7000 else GOLD_DIM if b > 4000 else '#4A3810'
              for b in users_sorted['balance']]
    fig = go.Figure(go.Bar(
        x=users_sorted['name'],
        y=users_sorted['balance'].round(2),
        marker=dict(color=colors, line=dict(color=BORDER, width=1)),
        text=[f'${b:,.0f}' for b in users_sorted['balance']],
        textposition='outside',
        textfont=dict(size=9, color=TEXT_DIM),
        hovertemplate='%{x}: $%{y:,.2f}<extra></extra>',
    ))
    fig.update_layout(**PLOT_LAYOUT, height=300,
        yaxis_tickprefix='$', yaxis_title='Balance')
    return fig


def fig_hist():
    fig = go.Figure(go.Histogram(
        x=amounts, nbinsx=20,
        marker=dict(color='rgba(201,168,76,0.5)',
                    line=dict(color=GOLD, width=1)),
        hovertemplate='$%{x}: %{y} transactions<extra></extra>',
    ))
    fig.update_layout(**PLOT_LAYOUT, height=260,
        xaxis_tickprefix='$', yaxis_title='Count',
        xaxis_title='Transaction Amount')
    return fig


def fig_scatter():
    fig = go.Figure(go.Scatter(
        x=incentive_data['amount'],
        y=incentive_data['incentive'],
        mode='markers',
        marker=dict(color=GREEN, size=7, opacity=0.7,
                    line=dict(color='rgba(76,175,130,0.3)', width=1)),
        hovertemplate='Amount: $%{x}<br>Incentive: $%{y}<extra></extra>',
    ))
    fig.update_layout(**PLOT_LAYOUT, height=260,
        xaxis_title='Transfer Amount ($)',
        yaxis_title='Incentive ($)')
    return fig


def fig_top_senders():
    fig = go.Figure(go.Bar(
        x=sender_vol['amount'].round(2),
        y=sender_vol['name'],
        orientation='h',
        marker=dict(color=BLUE, line=dict(color='rgba(42,100,150,0.4)', width=1)),
        text=[f'${v:,.0f}' for v in sender_vol['amount']],
        textposition='outside',
        textfont=dict(size=9, color=TEXT_DIM),
        hovertemplate='%{y}: $%{x:,.0f}<extra></extra>',
    ))
    fig.update_layout(**PLOT_LAYOUT, height=280,
        
        xaxis_tickprefix='$', xaxis_title='Total Sent')
    return fig


def fig_recv():
    fig = go.Figure(go.Bar(
        x=recv_vol['count'],
        y=recv_vol['name'],
        orientation='h',
        marker=dict(color=PURPLE, line=dict(color='rgba(124,92,191,0.4)', width=1)),
        text=recv_vol['count'],
        textposition='outside',
        textfont=dict(size=9, color=TEXT_DIM),
        hovertemplate='%{y}: %{x} received<extra></extra>',
    ))
    fig.update_layout(**PLOT_LAYOUT, height=280,
        
        xaxis_title='Transactions Received')
    return fig


# ── COMPONENTS ────────────────────────────────────────────────────────────────
def kpi_card(value, label, color=GOLD_BRIGHT):
    return html.Div([
        html.Span(str(value), style={
            'display': 'block', 'fontSize': '2.2rem', 'fontWeight': '600',
            'color': color, 'marginBottom': '6px',
        }),
        html.Span(label.upper(), style={
            'fontSize': '0.6rem', 'letterSpacing': '0.2em', 'color': TEXT_DIM,
        }),
    ], style={
        'background': BG2, 'padding': '1.8rem',
        'borderRight': f'1px solid {BORDER}', 'textAlign': 'center', 'flex': '1',
    })


def chart_card(title, subtitle, figure):
    return html.Div([
        html.Div(title.upper(), style={
            'fontSize': '0.65rem', 'letterSpacing': '0.2em',
            'color': GOLD, 'marginBottom': '4px',
        }),
        html.Div(subtitle, style={
            'fontFamily': 'Crimson Pro, serif', 'fontSize': '0.9rem',
            'fontStyle': 'italic', 'color': TEXT_DIM, 'marginBottom': '0.5rem',
        }),
        dcc.Graph(figure=figure, config={'displayModeBar': False}),
    ], style={
        'background': SURFACE, 'border': f'1px solid {BORDER}',
        'padding': '1.2rem', 'flex': '1',
    })


def pipe_box(icon, name, detail, color=GOLD):
    return html.Div([
        html.Div(icon, style={'fontSize': '1.4rem', 'marginBottom': '4px'}),
        html.Div(name.upper(), style={'fontSize': '0.6rem', 'letterSpacing': '0.15em', 'color': color, 'marginBottom': '2px'}),
        html.Div(detail, style={'fontFamily': 'Crimson Pro, serif', 'fontSize': '0.8rem', 'color': TEXT_DIM, 'fontStyle': 'italic'}),
    ], style={
        'background': SURFACE, 'border': f'1px solid {BORDER}',
        'padding': '1rem 1.2rem', 'minWidth': '120px', 'textAlign': 'center',
    })


arrow = html.Div('——→', style={'color': GOLD_DIM, 'padding': '0 6px', 'fontSize': '1rem', 'alignSelf': 'center', 'flexShrink': '0'})

# ── APP ───────────────────────────────────────────────────────────────────────
app = Dash(__name__)
app.title = 'Midas Extended — ML Data Engineering'

app.layout = html.Div(style={
    'background': BG, 'color': TEXT, 'minHeight': '100vh',
    'fontFamily': 'JetBrains Mono, monospace',
}, children=[

    html.Link(rel='preconnect', href='https://fonts.googleapis.com'),
    html.Link(rel='stylesheet', href='https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700;900&family=JetBrains+Mono:wght@300;400;600&family=Crimson+Pro:ital,wght@0,300;1,300&display=swap'),

    # ── HERO ──────────────────────────────────────────────────────────
    html.Div([
        html.Div('JPMorgan Forage · Extended · ML + Data Engineering', style={
            'fontSize': '0.65rem', 'letterSpacing': '0.2em', 'color': GOLD,
            'border': f'1px solid {GOLD_DIM}', 'padding': '6px 14px',
            'display': 'inline-block', 'marginBottom': '2rem',
        }),
        html.H1([
            html.Span('Midas', style={'display': 'block'}),
            html.Span('Extended', style={'color': GOLD, 'display': 'block'}),
        ], style={
            'fontFamily': 'Playfair Display, serif',
            'fontSize': 'clamp(4rem, 10vw, 8rem)',
            'fontWeight': '900', 'lineHeight': '0.9',
            'letterSpacing': '-0.02em', 'marginBottom': '1.5rem',
        }),
        html.P('The JPMorgan Forage foundation extended into a production grade ML and Data Engineering platform. Real Kafka pipelines, PostgreSQL persistence, Apache Superset dashboards, and a fraud detection engine.', style={
            'fontFamily': 'Crimson Pro, serif', 'fontSize': '1.2rem',
            'fontStyle': 'italic', 'color': TEXT_DIM,
            'maxWidth': '560px', 'lineHeight': '1.7', 'marginBottom': '2rem',
        }),
        html.Div([
            html.Div([html.Span(f'{TOTAL_USERS}', style={'fontSize': '2rem', 'color': GOLD_BRIGHT, 'display': 'block'}), html.Span('USERS', style={'fontSize': '0.6rem', 'color': TEXT_DIM, 'letterSpacing': '0.15em'})], style={'marginRight': '3rem'}),
            html.Div([html.Span(f'{TOTAL_TXN}', style={'fontSize': '2rem', 'color': GOLD_BRIGHT, 'display': 'block'}), html.Span('TRANSACTIONS', style={'fontSize': '0.6rem', 'color': TEXT_DIM, 'letterSpacing': '0.15em'})], style={'marginRight': '3rem'}),
            html.Div([html.Span(f'${TOTAL_VOLUME:,.0f}', style={'fontSize': '2rem', 'color': GOLD_BRIGHT, 'display': 'block'}), html.Span('TOTAL VOLUME', style={'fontSize': '0.6rem', 'color': TEXT_DIM, 'letterSpacing': '0.15em'})], style={'marginRight': '3rem'}),
            html.Div([html.Span('4', style={'fontSize': '2rem', 'color': GOLD_BRIGHT, 'display': 'block'}), html.Span('PHASES BUILT', style={'fontSize': '0.6rem', 'color': TEXT_DIM, 'letterSpacing': '0.15em'})]),
        ], style={'display': 'flex', 'alignItems': 'flex-start'}),
    ], style={
        'padding': '8rem 8vw 5rem',
        'borderBottom': f'1px solid {BORDER}',
        'background': 'radial-gradient(ellipse 80% 60% at 70% 50%, rgba(201,168,76,0.05) 0%, transparent 60%)',
    }),

    # ── PIPELINE ARCHITECTURE ─────────────────────────────────────────
    html.Div([
        html.Div('// 01 — ARCHITECTURE', style={'fontSize': '0.65rem', 'letterSpacing': '0.25em', 'color': GOLD, 'marginBottom': '0.75rem'}),
        html.H2('The Production Pipeline', style={'fontFamily': 'Playfair Display, serif', 'fontSize': 'clamp(2rem, 4vw, 3rem)', 'marginBottom': '0.75rem'}),
        html.P('Every transaction flows through a battle-tested pipeline. Python generates synthetic data, Kafka brokers it, Spring Boot consumes and validates, PostgreSQL persists, and Superset visualises.', style={
            'fontFamily': 'Crimson Pro, serif', 'fontSize': '1.05rem',
            'fontStyle': 'italic', 'color': TEXT_DIM, 'maxWidth': '640px',
            'lineHeight': '1.8', 'marginBottom': '2.5rem',
        }),
        html.Div([
            pipe_box('🐍', 'Python', 'Data Generator', GREEN),
            arrow,
            pipe_box('📨', 'Kafka', 'trader-updates', GOLD),
            arrow,
            pipe_box('☕', 'Spring Boot', '@KafkaListener', BLUE),
            arrow,
            pipe_box('✅', 'Validation', 'Balance check', GOLD),
            arrow,
            pipe_box('🎁', 'Incentive', 'IncentiveService', PURPLE),
            arrow,
            pipe_box('🐘', 'PostgreSQL', 'Flyway + JPA', GREEN),
            arrow,
            pipe_box('📊', 'Superset', 'Live Dashboard', GOLD),
        ], style={'display': 'flex', 'alignItems': 'center', 'overflowX': 'auto', 'paddingBottom': '1rem'}),
    ], style={'padding': '5rem 8vw', 'borderBottom': f'1px solid {BORDER}'}),

    # ── KPI ROW ───────────────────────────────────────────────────────
    html.Div([
        html.Div('// 02 — LIVE METRICS', style={'fontSize': '0.65rem', 'letterSpacing': '0.25em', 'color': GOLD, 'marginBottom': '2rem'}),
        html.Div([
            kpi_card(TOTAL_TXN, 'Transactions Processed', GOLD_BRIGHT),
            kpi_card(f'${AVG_TXN_AMOUNT:,.0f}', 'Avg Transaction', GREEN),
            kpi_card(f'${TOTAL_INCENTIVE:,.0f}', 'Incentives Paid', PURPLE),
            kpi_card(f'${TOTAL_VOLUME:,.0f}', 'Total Volume', BLUE),
        ], style={'display': 'flex', 'border': f'1px solid {BORDER}'}),
    ], style={'padding': '5rem 8vw', 'borderBottom': f'1px solid {BORDER}'}),

    # ── CHARTS ────────────────────────────────────────────────────────
    html.Div([
        html.Div('// 03 — ANALYTICS', style={'fontSize': '0.65rem', 'letterSpacing': '0.25em', 'color': GOLD, 'marginBottom': '0.75rem'}),
        html.H2('Transaction Analytics', style={'fontFamily': 'Playfair Display, serif', 'fontSize': 'clamp(2rem, 4vw, 3rem)', 'marginBottom': '0.75rem'}),
        html.P(f'Real data from PostgreSQL. {TOTAL_TXN} transactions across {TOTAL_USERS} users processed through the Kafka pipeline.', style={
            'fontFamily': 'Crimson Pro, serif', 'fontSize': '1.05rem',
            'fontStyle': 'italic', 'color': TEXT_DIM, 'marginBottom': '2rem',
        }),

        html.Div([
            chart_card('Kafka Processing Volume', 'Transactions processed per Kafka batch', fig_volume()),
        ], style={'display': 'flex', 'marginBottom': '1.5rem'}),

        html.Div([
            chart_card('User Final Balances', 'Real PostgreSQL balances after all transactions', fig_balances()),
        ], style={'display': 'flex', 'marginBottom': '1.5rem'}),

        html.Div([
            chart_card('Transaction Amount Distribution', 'Histogram of all transfer amounts', fig_hist()),
            chart_card('Incentive vs Transfer Amount', 'Correlation between transfer size and incentive', fig_scatter()),
        ], style={'display': 'flex', 'gap': '1.5rem', 'marginBottom': '1.5rem'}),

        html.Div([
            chart_card('Top Senders by Volume', 'Total amount sent per user', fig_top_senders()),
            chart_card('Top Recipients by Count', 'Most frequently receiving users', fig_recv()),
        ], style={'display': 'flex', 'gap': '1.5rem'}),

    ], style={'padding': '5rem 8vw', 'borderBottom': f'1px solid {BORDER}'}),

    # ── PHASE ROADMAP ─────────────────────────────────────────────────
    html.Div([
        html.Div('// 04 — ROADMAP', style={'fontSize': '0.65rem', 'letterSpacing': '0.25em', 'color': GOLD, 'marginBottom': '0.75rem'}),
        html.H2('Built in Phases', style={'fontFamily': 'Playfair Display, serif', 'fontSize': 'clamp(2rem, 4vw, 3rem)', 'marginBottom': '2rem'}),
        html.Div([
            html.Div([
                html.Div('PHASE 01', style={'fontSize': '0.6rem', 'color': GOLD_DIM, 'letterSpacing': '0.2em', 'marginBottom': '6px'}),
                html.Div('Code Cleanup', style={'fontFamily': 'Playfair Display, serif', 'fontSize': '1.1rem', 'marginBottom': '6px'}),
                html.Div('float to BigDecimal, @Transactional, SLF4J logging, GlobalExceptionHandler', style={'fontFamily': 'Crimson Pro, serif', 'fontSize': '0.85rem', 'color': TEXT_DIM, 'fontStyle': 'italic', 'lineHeight': '1.6'}),
                html.Div('COMPLETE', style={'fontSize': '0.6rem', 'color': GREEN, 'marginTop': '10px', 'letterSpacing': '0.15em'}),
            ], style={'background': BG2, 'padding': '1.5rem', 'borderLeft': f'3px solid {GREEN}', 'flex': '1'}),
            html.Div([
                html.Div('PHASE 02', style={'fontSize': '0.6rem', 'color': GOLD_DIM, 'letterSpacing': '0.2em', 'marginBottom': '6px'}),
                html.Div('Data Engineering', style={'fontFamily': 'Playfair Display, serif', 'fontSize': '1.1rem', 'marginBottom': '6px'}),
                html.Div('PostgreSQL + Flyway, Kafka pipeline, ETL scripts, EDA notebook, Superset dashboard', style={'fontFamily': 'Crimson Pro, serif', 'fontSize': '0.85rem', 'color': TEXT_DIM, 'fontStyle': 'italic', 'lineHeight': '1.6'}),
                html.Div('COMPLETE', style={'fontSize': '0.6rem', 'color': GREEN, 'marginTop': '10px', 'letterSpacing': '0.15em'}),
            ], style={'background': BG2, 'padding': '1.5rem', 'borderLeft': f'3px solid {GREEN}', 'flex': '1'}),
            html.Div([
                html.Div('PHASE 03', style={'fontSize': '0.6rem', 'color': GOLD_DIM, 'letterSpacing': '0.2em', 'marginBottom': '6px'}),
                html.Div('Fraud Detection ML', style={'fontFamily': 'Playfair Display, serif', 'fontSize': '1.1rem', 'marginBottom': '6px'}),
                html.Div('XGBoost + Isolation Forest, FastAPI inference service, Spring Boot integration, fraud scoring', style={'fontFamily': 'Crimson Pro, serif', 'fontSize': '0.85rem', 'color': TEXT_DIM, 'fontStyle': 'italic', 'lineHeight': '1.6'}),
                html.Div('IN PROGRESS', style={'fontSize': '0.6rem', 'color': GOLD, 'marginTop': '10px', 'letterSpacing': '0.15em'}),
            ], style={'background': BG2, 'padding': '1.5rem', 'borderLeft': f'3px solid {GOLD}', 'flex': '1'}),
            html.Div([
                html.Div('PHASE 04', style={'fontSize': '0.6rem', 'color': GOLD_DIM, 'letterSpacing': '0.2em', 'marginBottom': '6px'}),
                html.Div('MLOps', style={'fontFamily': 'Playfair Display, serif', 'fontSize': '1.1rem', 'marginBottom': '6px'}),
                html.Div('PSI drift monitoring, model registry, Prometheus + Grafana, SHAP model cards', style={'fontFamily': 'Crimson Pro, serif', 'fontSize': '0.85rem', 'color': TEXT_DIM, 'fontStyle': 'italic', 'lineHeight': '1.6'}),
                html.Div('UPCOMING', style={'fontSize': '0.6rem', 'color': TEXT_DIM, 'marginTop': '10px', 'letterSpacing': '0.15em'}),
            ], style={'background': BG2, 'padding': '1.5rem', 'borderLeft': f'3px solid {TEXT_DIM}', 'flex': '1'}),
        ], style={'display': 'flex', 'gap': '1px', 'background': BORDER}),
    ], style={'padding': '5rem 8vw', 'borderBottom': f'1px solid {BORDER}'}),

    # ── FOOTER ────────────────────────────────────────────────────────
    html.Div([
        html.Span('Midas Extended', style={'fontFamily': 'Playfair Display, serif', 'fontSize': '1.5rem', 'color': GOLD}),
        html.Div([
            html.A('GitHub', href='https://github.com/ahwan-0/midas-extended', target='_blank',
                   style={'color': TEXT_DIM, 'textDecoration': 'none', 'fontSize': '0.65rem', 'letterSpacing': '0.15em', 'marginRight': '2rem'}),
            html.A('Midas Base', href='https://github.com/ahwan-0/midas', target='_blank',
                   style={'color': TEXT_DIM, 'textDecoration': 'none', 'fontSize': '0.65rem', 'letterSpacing': '0.15em'}),
        ]),
    ], style={
        'padding': '2.5rem 8vw', 'display': 'flex',
        'justifyContent': 'space-between', 'alignItems': 'center',
    }),
])

server = app.server

if __name__ == '__main__':
    app.run(debug=True)
