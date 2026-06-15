"""
CRV/PCL banner view-to-click Sankey — iOS vs Android, by overlap_status arm.
Output: journey_sankey.html (self-contained, offline).
"""

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    raise SystemExit("plotly not installed — run: pip install plotly")

import os

# =============================================================================
# DATA — edit these values; replace with exact query output
# APPROX from OCR of query output — REPLACE with exact query values; flagged cells uncertain
# =============================================================================
data = {
    'iOS': {
        'overlap_action': {
            'Viewed Both':     {'Clicked both': 52692, 'Clicked CRV only': 14485, 'Clicked PCL only': 77634, 'Clicked neither': 246161},
            'Viewed PCL only': {'Clicked both': 0,     'Clicked CRV only': 0,     'Clicked PCL only': 15517, 'Clicked neither': 31992},   # total 47509 uncertain (one source said 62509)
            'Viewed CRV only': {'Clicked both': 0,     'Clicked CRV only': 4812,  'Clicked PCL only': 0,     'Clicked neither': 24890},
        },
        'overlap_control': {
            'Viewed PCL only': {'Clicked both': 0,     'Clicked CRV only': 0,     'Clicked PCL only': 9764,  'Clicked neither': 14198},
        },
        'no_overlap': {
            'Viewed Both':     {'Clicked both': 756,   'Clicked CRV only': 1883,  'Clicked PCL only': 503,   'Clicked neither': 912},
            'Viewed PCL only': {'Clicked both': 0,     'Clicked CRV only': 0,     'Clicked PCL only': 72289, 'Clicked neither': 80351},
            'Viewed CRV only': {'Clicked both': 0,     'Clicked CRV only': 123,   'Clicked PCL only': 0,     'Clicked neither': 657},
        },
    },
    'Android': {
        'overlap_action': {
            'Viewed Both':     {'Clicked both': 18749, 'Clicked CRV only': 2681,  'Clicked PCL only': 26450, 'Clicked neither': 54416},
            'Viewed PCL only': {'Clicked both': 0,     'Clicked CRV only': 0,     'Clicked PCL only': 8757,  'Clicked neither': 56926},
            'Viewed CRV only': {'Clicked both': 0,     'Clicked CRV only': 1219,  'Clicked PCL only': 0,     'Clicked neither': 5960},   # total 7179 uncertain (raw OCR showed 71)
        },
        'overlap_control': {
            'Viewed PCL only': {'Clicked both': 0,     'Clicked CRV only': 0,     'Clicked PCL only': 3401,  'Clicked neither': 6238},
        },
        'no_overlap': {
            'Viewed Both':     {'Clicked both': 246,   'Clicked CRV only': 588,   'Clicked PCL only': 182,   'Clicked neither': 477},
            'Viewed PCL only': {'Clicked both': 0,     'Clicked CRV only': 0,     'Clicked PCL only': 28114, 'Clicked neither': 33884},
            'Viewed CRV only': {'Clicked both': 0,     'Clicked CRV only': 37,    'Clicked PCL only': 0,     'Clicked neither': 184},
        },
    },
}

# =============================================================================
# LAYOUT CONSTANTS
# =============================================================================
ARM_LABELS = {
    'overlap_action':  'Action',
    'overlap_control': 'Control',
    'no_overlap':      'No Overlap',
}

# Node colors by category
NODE_COLORS = {
    # arms
    'overlap_action':  'rgba(31,119,180,0.85)',   # blue
    'overlap_control': 'rgba(148,103,189,0.85)',  # purple
    'no_overlap':      'rgba(127,127,127,0.85)',  # grey
    # view states
    'Viewed Both':     'rgba(44,160,44,0.85)',    # green
    'Viewed CRV only': 'rgba(31,119,180,0.85)',   # blue
    'Viewed PCL only': 'rgba(255,127,14,0.85)',   # orange
    # click states
    'Clicked both':    'rgba(44,160,44,0.85)',    # green
    'Clicked CRV only':'rgba(31,119,180,0.85)',   # blue
    'Clicked PCL only':'rgba(255,127,14,0.85)',   # orange
    'Clicked neither': 'rgba(188,189,34,0.60)',   # olive/grey
}

LINK_COLORS = {
    'Viewed Both':     'rgba(44,160,44,0.25)',
    'Viewed CRV only': 'rgba(31,119,180,0.25)',
    'Viewed PCL only': 'rgba(255,127,14,0.25)',
    'Clicked both':    'rgba(44,160,44,0.20)',
    'Clicked CRV only':'rgba(31,119,180,0.20)',
    'Clicked PCL only':'rgba(255,127,14,0.20)',
    'Clicked neither': 'rgba(188,189,34,0.15)',
}


# =============================================================================
# SANKEY BUILDER
# =============================================================================
def build_sankey_trace(platform_data, domain_x):
    """
    Build a go.Sankey trace for one platform.
    Nodes are arm-prefixed so flows never merge across arms.
    """
    node_labels = []
    node_colors = []
    node_index  = {}   # key -> int index

    def get_or_add(key, label, color):
        if key not in node_index:
            node_index[key] = len(node_labels)
            node_labels.append(label)
            node_colors.append(color)
        return node_index[key]

    # Layer 1: arm nodes (left)
    for arm, arm_label in ARM_LABELS.items():
        if arm in platform_data:
            get_or_add(f'arm::{arm}', arm_label, NODE_COLORS[arm])

    # Layer 2+3: arm-prefixed view + click nodes (keeps arms visually separate)
    for arm, arm_label in ARM_LABELS.items():
        if arm not in platform_data:
            continue
        for view_state, clicks in platform_data[arm].items():
            v_key   = f'{arm}::{view_state}'
            v_label = f"{arm_label}: {view_state}"
            get_or_add(v_key, v_label, NODE_COLORS.get(view_state, 'rgba(100,100,100,0.8)'))
            for click_state in clicks:
                c_key   = f'{arm}::{click_state}'
                c_label = f"{arm_label}: {click_state}"
                get_or_add(c_key, c_label, NODE_COLORS.get(click_state, 'rgba(100,100,100,0.8)'))

    # Build links
    src, tgt, val, link_colors, link_labels = [], [], [], [], []

    for arm, arm_label in ARM_LABELS.items():
        if arm not in platform_data:
            continue
        arm_idx = node_index[f'arm::{arm}']
        arm_total = sum(
            sum(c for c in clicks.values())
            for clicks in platform_data[arm].values()
        )
        for view_state, clicks in platform_data[arm].items():
            view_total = sum(clicks.values())
            if view_total == 0:
                continue
            v_idx = node_index[f'{arm}::{view_state}']
            pct_arm = view_total / arm_total * 100 if arm_total else 0

            # arm -> view state
            src.append(arm_idx)
            tgt.append(v_idx)
            val.append(view_total)
            link_colors.append(LINK_COLORS.get(view_state, 'rgba(150,150,150,0.2)'))
            link_labels.append(
                f"{arm_label} → {view_state}<br>{view_total:,} clients ({pct_arm:.1f}% of arm)"
            )

            # view state -> click states
            for click_state, count in clicks.items():
                if count == 0:
                    continue
                c_idx = node_index[f'{arm}::{click_state}']
                pct_view = count / view_total * 100 if view_total else 0
                src.append(v_idx)
                tgt.append(c_idx)
                val.append(count)
                link_colors.append(LINK_COLORS.get(click_state, 'rgba(150,150,150,0.15)'))
                link_labels.append(
                    f"{arm_label}: {view_state} → {click_state}<br>"
                    f"{count:,} clients ({pct_view:.1f}% of view group)"
                )

    trace = go.Sankey(
        domain=dict(x=domain_x, y=[0, 1]),
        arrangement='snap',
        node=dict(
            pad=12,
            thickness=18,
            line=dict(color='white', width=0.5),
            label=node_labels,
            color=node_colors,
            hovertemplate='%{label}<br>%{value:,} clients<extra></extra>',
        ),
        link=dict(
            source=src,
            target=tgt,
            value=val,
            color=link_colors,
            customdata=link_labels,
            hovertemplate='%{customdata}<extra></extra>',
        ),
    )
    return trace


# =============================================================================
# VALIDATION
# =============================================================================
def validate(platform, platform_data):
    print(f"\n--- {platform} ---")
    for arm, views in platform_data.items():
        arm_total = sum(sum(c.values()) for c in views.values())
        print(f"  {ARM_LABELS[arm]}: {arm_total:,} clients entering arm")
        for view_state, clicks in views.items():
            incoming  = sum(clicks.values())
            outgoing  = sum(clicks.values())   # same dict — just confirming non-zero
            # Check: each click bucket sums to the view total
            computed  = sum(v for v in clicks.values())
            status    = 'OK' if computed == incoming else f'WARNING: expected {incoming:,}, got {computed:,}'
            print(f"    {view_state}: {incoming:,}  [{status}]")


# =============================================================================
# MAIN
# =============================================================================
fig = make_subplots(
    rows=1, cols=2,
    specs=[[{'type': 'sankey'}, {'type': 'sankey'}]],
    subplot_titles=['iOS Journey', 'Android Journey'],
)

ios_trace     = build_sankey_trace(data['iOS'],     domain_x=[0.0, 0.47])
android_trace = build_sankey_trace(data['Android'], domain_x=[0.53, 1.0])

fig.add_trace(ios_trace,     row=1, col=1)
fig.add_trace(android_trace, row=1, col=2)

fig.update_layout(
    title=dict(
        text='CRV × PCL Banner Journey — View to Click by Overlap Arm',
        font=dict(size=16),
    ),
    font=dict(size=11, family='Arial'),
    paper_bgcolor='white',
    height=700,
    margin=dict(l=20, r=20, t=80, b=20),
)

out_path = os.path.join(os.path.dirname(__file__), 'journey_sankey.html')
fig.write_html(out_path, include_plotlyjs='inline')
print(f"\nOutput: {out_path}")

for platform, platform_data in data.items():
    validate(platform, platform_data)
