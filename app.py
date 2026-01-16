"""
HRMS Semantic Layer - Web UI
Streamlit dashboard for exploring HRMS data
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Page config
st.set_page_config(
    page_title="HRMS Dashboard",
    page_icon="👥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def get_connection():
    db_path = Path(__file__).parent / "hrmsdb.duckdb"
    if not db_path.exists():
        st.error("Database not found. Run `python init_semantic_layer.py` first.")
        st.stop()
    return duckdb.connect(str(db_path), read_only=True)

conn = get_connection()

# Helper function to run queries
@st.cache_data(ttl=300)
def run_query(query):
    return conn.execute(query).fetchdf()

# Sidebar navigation
st.sidebar.title("HRMS Dashboard")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Employees", "Benefits", "Attendance", "Activity Log"]
)

# Overview Page
if page == "Overview":
    st.title("HRMS Overview")

    # Key metrics in columns
    col1, col2, col3, col4 = st.columns(4)

    # Get counts
    employee_count = run_query("SELECT COUNT(DISTINCT employee_id) as cnt FROM staging.stg_payroll")['cnt'][0]
    payroll_records = run_query("SELECT COUNT(*) as cnt FROM raw.crmc_payrollfile")['cnt'][0]
    attendance_records = run_query("SELECT COUNT(*) as cnt FROM staging.stg_attendance")['cnt'][0]
    activity_records = run_query("SELECT COUNT(*) as cnt FROM raw.activity_log")['cnt'][0]

    col1.metric("Employees", f"{employee_count:,}")
    col2.metric("Payroll Records", f"{payroll_records:,}")
    col3.metric("Attendance Records", f"{attendance_records:,}")
    col4.metric("Activity Logs", f"{activity_records:,}")

    st.divider()

    # Benefits enrollment chart
    st.subheader("Benefits Enrollment by Plan Type")
    benefits_df = run_query("""
        SELECT benefit_plan_type, employee_count, enrollment_records
        FROM metrics.headcount_metrics
        ORDER BY employee_count DESC
        LIMIT 10
    """)

    fig = px.bar(
        benefits_df,
        x='benefit_plan_type',
        y='employee_count',
        color='enrollment_records',
        labels={'benefit_plan_type': 'Plan Type', 'employee_count': 'Employees'},
        color_continuous_scale='Blues'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

    # Attendance by shift
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Attendance by Shift")
        shift_df = run_query("""
            SELECT shift, SUM(total_records) as records, SUM(total_hours) as hours
            FROM metrics.attendance_metrics
            GROUP BY shift
            ORDER BY records DESC
        """)
        fig = px.pie(shift_df, values='records', names='shift', hole=0.4)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Hours by Shift")
        fig = px.bar(shift_df, x='shift', y='hours', color='shift')
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

# Employees Page
elif page == "Employees":
    st.title("Employee Directory")

    # Search
    search = st.text_input("Search by name or ID", "")

    # Get employees
    if search:
        query = f"""
            SELECT employee_id, full_name, num_benefit_plans, benefit_plan_types,
                   total_current_arrears, corp_id
            FROM business.employee_summary
            WHERE LOWER(full_name) LIKE '%{search.lower()}%'
               OR employee_id LIKE '%{search}%'
            ORDER BY full_name
            LIMIT 100
        """
    else:
        query = """
            SELECT employee_id, full_name, num_benefit_plans, benefit_plan_types,
                   total_current_arrears, corp_id
            FROM business.employee_summary
            ORDER BY full_name
            LIMIT 100
        """

    employees_df = run_query(query)

    st.dataframe(
        employees_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "employee_id": "Employee ID",
            "full_name": "Name",
            "num_benefit_plans": "# Plans",
            "benefit_plan_types": st.column_config.TextColumn("Plan Types", width="large"),
            "total_current_arrears": st.column_config.NumberColumn("Arrears", format="$%.2f"),
            "corp_id": "Corp ID"
        }
    )

    st.caption(f"Showing {len(employees_df)} employees")

# Benefits Page
elif page == "Benefits":
    st.title("Benefits Analysis")

    # Benefits metrics
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Enrollment by Plan Type")
        metrics_df = run_query("""
            SELECT benefit_plan_type, employee_count, enrollment_records,
                   ROUND(avg_current_arrears, 2) as avg_arrears
            FROM metrics.headcount_metrics
            ORDER BY employee_count DESC
        """)
        st.dataframe(metrics_df, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Top Plans by Enrollment")
        fig = px.treemap(
            metrics_df.head(10),
            path=['benefit_plan_type'],
            values='employee_count',
            color='avg_arrears',
            color_continuous_scale='RdYlGn_r'
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Detailed benefits view
    st.subheader("Benefits Detail")

    plan_filter = st.selectbox(
        "Filter by Plan Type",
        ["All"] + list(metrics_df['benefit_plan_type'].unique())
    )

    if plan_filter == "All":
        detail_query = """
            SELECT employee_id, full_name, benefit_plan_type, benefit_plan_name,
                   coverage_tier, current_arrears, total_arrears
            FROM business.payroll_detail
            LIMIT 500
        """
    else:
        detail_query = f"""
            SELECT employee_id, full_name, benefit_plan_type, benefit_plan_name,
                   coverage_tier, current_arrears, total_arrears
            FROM business.payroll_detail
            WHERE benefit_plan_type = '{plan_filter}'
            LIMIT 500
        """

    detail_df = run_query(detail_query)
    st.dataframe(detail_df, use_container_width=True, hide_index=True)

# Attendance Page
elif page == "Attendance":
    st.title("Attendance Analysis")

    # Attendance metrics
    attendance_df = run_query("""
        SELECT week_number, shift, employee_count, total_records,
               total_hours, avg_hours, shift_diff_hours
        FROM metrics.attendance_metrics
        ORDER BY week_number, shift
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Hours by Week and Shift")
        fig = px.bar(
            attendance_df,
            x='week_number',
            y='total_hours',
            color='shift',
            barmode='group'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Employee Count by Shift")
        shift_summary = attendance_df.groupby('shift').agg({
            'employee_count': 'sum',
            'total_hours': 'sum'
        }).reset_index()
        fig = px.bar(shift_summary, x='shift', y='employee_count', color='shift')
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Detailed attendance
    st.subheader("Attendance Details")
    detail_df = run_query("""
        SELECT employee_id, full_name, shift, week_number,
               total_hours, hours, rate, earning_code
        FROM business.attendance_detail
        ORDER BY employee_id, week_number
        LIMIT 500
    """)
    st.dataframe(detail_df, use_container_width=True, hide_index=True)

# Activity Log Page
elif page == "Activity Log":
    st.title("Activity Log")

    st.info("Showing activity from the last 30 days")

    # Activity summary
    activity_df = run_query("""
        SELECT activity_id, activity_timestamp, module,
               activity_type, entered_by, system_ip
        FROM staging.stg_activity_log
        ORDER BY activity_timestamp DESC
        LIMIT 100
    """)

    if len(activity_df) > 0:
        # Activity by module
        module_counts = activity_df['module'].value_counts().reset_index()
        module_counts.columns = ['module', 'count']

        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader("Activity by Module")
            fig = px.pie(module_counts, values='count', names='module')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Recent Activity")
            st.dataframe(
                activity_df[['activity_timestamp', 'module', 'activity_type', 'entered_by']],
                use_container_width=True,
                hide_index=True
            )
    else:
        st.warning("No activity log data available")

# Footer
st.sidebar.divider()
st.sidebar.caption("HRMS Semantic Layer v1.0")
st.sidebar.caption("Data: DuckDB + SQL Server")
