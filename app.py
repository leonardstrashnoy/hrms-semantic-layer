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
import ollama

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
    db_path = Path(__file__).parent / "data" / "hrmsdb.duckdb"
    if not db_path.exists():
        st.error("Database not found. Run `python init_semantic_layer.py` first.")
        st.stop()
    conn = duckdb.connect(str(db_path), read_only=True)
    conn.execute("LOAD fts; LOAD excel; LOAD vss; LOAD httpfs;")
    return conn

conn = get_connection()

# Helper function to run queries
@st.cache_data(ttl=300)
def run_query(query):
    return conn.execute(query).fetchdf()

def run_query_safe(query, params):
    """Run a parameterized query (not cached due to dynamic params)."""
    return conn.execute(query, params).fetchdf()

# Sidebar navigation
st.sidebar.title("HRMS Dashboard")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Employees", "Benefits", "Attendance", "Activity Log", "AI Query"]
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
    st.plotly_chart(fig, width='stretch')

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
        st.plotly_chart(fig, width='stretch')

    with col2:
        st.subheader("Hours by Shift")
        fig = px.bar(shift_df, x='shift', y='hours', color='shift')
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, width='stretch')

# Employees Page
elif page == "Employees":
    st.title("Employee Directory")

    # Search
    search = st.text_input("Search by name or ID", "")

    # Get employees
    if search:
        query = """
            SELECT employee_id, full_name, num_benefit_plans, benefit_plan_types,
                   total_current_arrears, corp_id
            FROM business.employee_summary
            WHERE LOWER(full_name) LIKE LOWER(?)
               OR employee_id LIKE ?
            ORDER BY full_name
            LIMIT 100
        """
        search_pattern = f"%{search}%"
        employees_df = run_query_safe(query, [search_pattern, search_pattern])
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
        width='stretch',
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
        st.dataframe(metrics_df, width='stretch', hide_index=True)

    with col2:
        st.subheader("Top Plans by Enrollment")
        fig = px.treemap(
            metrics_df.head(10),
            path=['benefit_plan_type'],
            values='employee_count',
            color='avg_arrears',
            color_continuous_scale='RdYlGn_r'
        )
        st.plotly_chart(fig, width='stretch')

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
        detail_df = run_query(detail_query)
    else:
        detail_query = """
            SELECT employee_id, full_name, benefit_plan_type, benefit_plan_name,
                   coverage_tier, current_arrears, total_arrears
            FROM business.payroll_detail
            WHERE benefit_plan_type = ?
            LIMIT 500
        """
        detail_df = run_query_safe(detail_query, [plan_filter])
    st.dataframe(detail_df, width='stretch', hide_index=True)

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
        st.plotly_chart(fig, width='stretch')

    with col2:
        st.subheader("Employee Count by Shift")
        shift_summary = attendance_df.groupby('shift').agg({
            'employee_count': 'sum',
            'total_hours': 'sum'
        }).reset_index()
        fig = px.bar(shift_summary, x='shift', y='employee_count', color='shift')
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, width='stretch')

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
    st.dataframe(detail_df, width='stretch', hide_index=True)

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
            st.plotly_chart(fig, width='stretch')

        with col2:
            st.subheader("Recent Activity")
            st.dataframe(
                activity_df[['activity_timestamp', 'module', 'activity_type', 'entered_by']],
                width='stretch',
                hide_index=True
            )
    else:
        st.warning("No activity log data available")

# AI Query Page
elif page == "AI Query":
    st.title("AI-Powered Query")
    st.caption("Ask questions about your HRMS data in natural language")

    # Get available models
    @st.cache_data(ttl=60)
    def get_ollama_models():
        try:
            models = ollama.list()
            return [m.model for m in models.models]
        except Exception as e:
            return []

    models = get_ollama_models()

    if not models:
        st.error("No Ollama models found. Make sure Ollama is running.")
        st.code("ollama serve", language="bash")
    else:
        # Model selection in sidebar for cleaner UI
        selected_model = st.sidebar.selectbox("AI Model", models)

        # Build schema context for the model
        @st.cache_data
        def get_schema_context():
            schema_df = conn.execute("""
                SELECT table_schema || '.' || table_name as table_name,
                       STRING_AGG(column_name || ' (' || data_type || ')', ', ') as columns
                FROM information_schema.columns
                WHERE table_schema IN ('staging', 'business', 'metrics')
                GROUP BY table_schema, table_name
                ORDER BY table_schema, table_name
            """).fetchdf()
            context = "Available tables and columns:\n\n"
            for _, row in schema_df.iterrows():
                context += f"- {row['table_name']}: {row['columns']}\n"
            return context

        schema_context = get_schema_context()

        # Session state initialization
        if 'generated_sql' not in st.session_state:
            st.session_state.generated_sql = ""
        if 'query_result' not in st.session_state:
            st.session_state.query_result = None
        if 'query_error' not in st.session_state:
            st.session_state.query_error = None

        # Query input
        user_question = st.text_input(
            "Ask a question about your HRMS data",
            placeholder="e.g., How many employees are enrolled in each benefit plan type?"
        )

        ask_btn = st.button("Ask", type="primary", use_container_width=True)

        if ask_btn and user_question:
            with st.spinner(f"Generating and running query..."):
                prompt = f"""You are a SQL expert. Generate a DuckDB SQL query to answer the user's question.

{schema_context}

Important notes:
- Use DuckDB SQL syntax
- Only use tables and columns from the schema above
- Return ONLY the SQL query, no explanations
- Use appropriate JOINs when needed
- Limit results to 100 rows unless the user asks for more

User question: {user_question}

SQL query:"""

                try:
                    response = ollama.generate(model=selected_model, prompt=prompt)
                    generated_sql = response.response.strip()
                    # Clean up the response - remove markdown code blocks if present
                    if generated_sql.startswith("```"):
                        lines = generated_sql.split("\n")
                        generated_sql = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
                    st.session_state.generated_sql = generated_sql
                    st.session_state.query_error = None

                    # Execute immediately
                    result_df = conn.execute(generated_sql).fetchdf()
                    st.session_state.query_result = result_df
                except Exception as e:
                    st.session_state.query_error = str(e)
                    st.session_state.query_result = None

        # Show results first (primary focus)
        if st.session_state.query_result is not None:
            result_df = st.session_state.query_result
            st.success(f"Query returned {len(result_df)} rows")
            st.dataframe(result_df, use_container_width=True, hide_index=True)

            # Offer to visualize if numeric columns exist
            numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
            if len(numeric_cols) > 0 and len(result_df) > 1:
                st.subheader("Quick Visualization")
                col1, col2, col3 = st.columns(3)
                with col1:
                    chart_type = st.selectbox("Chart Type", ["Bar", "Line", "Pie"])
                with col2:
                    all_cols = result_df.columns.tolist()
                    x_col = st.selectbox("X-axis / Labels", all_cols)
                with col3:
                    y_col = st.selectbox("Y-axis / Values", numeric_cols)

                if chart_type == "Bar":
                    fig = px.bar(result_df, x=x_col, y=y_col)
                elif chart_type == "Line":
                    fig = px.line(result_df, x=x_col, y=y_col)
                else:
                    fig = px.pie(result_df, names=x_col, values=y_col)

                st.plotly_chart(fig, use_container_width=True)

        # Show error if any
        if st.session_state.query_error:
            st.error(f"Query error: {st.session_state.query_error}")

        # Show SQL in expander (secondary - for review/modification)
        if st.session_state.generated_sql:
            with st.expander("Review / Modify SQL", expanded=st.session_state.query_error is not None):
                edited_sql = st.text_area(
                    "SQL Query",
                    value=st.session_state.generated_sql,
                    height=150,
                    label_visibility="collapsed"
                )

                if st.button("Re-run Modified Query"):
                    with st.spinner("Running query..."):
                        try:
                            result_df = conn.execute(edited_sql).fetchdf()
                            st.session_state.query_result = result_df
                            st.session_state.generated_sql = edited_sql
                            st.session_state.query_error = None
                            st.rerun()
                        except Exception as e:
                            st.error(f"Query error: {e}")

        # Schema reference at bottom
        with st.expander("View Database Schema"):
            schema_info = run_query("""
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema IN ('staging', 'business', 'metrics')
                ORDER BY table_schema, table_name, ordinal_position
            """)
            st.dataframe(schema_info, use_container_width=True, hide_index=True)

# Footer
st.sidebar.divider()
st.sidebar.caption("HRMS Semantic Layer v1.0")
st.sidebar.caption("Data: DuckDB + SQL Server")
