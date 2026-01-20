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
    # Install and load extensions (safe to call even if already installed)
    for ext in ['fts', 'excel', 'vss', 'httpfs']:
        try:
            conn.execute(f"INSTALL {ext}; LOAD {ext};")
        except Exception:
            pass  # Extension may already be loaded or not needed
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
    ["Executive Dashboard", "Workforce Demographics", "Detailed Attendance", "Payroll Analytics", "Employees", "Benefits", "Activity Log", "AI Query"]
)

# Executive Dashboard Page
if page == "Executive Dashboard":
    st.title("Executive Dashboard")

    # Try to get KPIs from the metrics view
    try:
        kpis = run_query("SELECT * FROM metrics.executive_kpis LIMIT 1")
        has_kpis = len(kpis) > 0
    except Exception:
        has_kpis = False

    if has_kpis:
        kpi_row = kpis.iloc[0]

        # KPI Cards Row
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Active Employees",
                f"{int(kpi_row.get('active_employees', 0)):,}",
                delta=f"+{int(kpi_row.get('new_hires_30d', 0))} new (30d)" if kpi_row.get('new_hires_30d', 0) > 0 else None
            )

        with col2:
            attendance_rate = kpi_row.get('attendance_rate_30d', 0)
            st.metric(
                "Attendance Rate (30d)",
                f"{attendance_rate:.1f}%",
                delta=f"{int(kpi_row.get('late_arrivals_30d', 0))} late arrivals" if kpi_row.get('late_arrivals_30d', 0) > 0 else None,
                delta_color="inverse"
            )

        with col3:
            latest_payroll = kpi_row.get('latest_gross_payroll', 0)
            st.metric(
                "Latest Payroll",
                f"${latest_payroll:,.0f}",
                delta=f"{int(kpi_row.get('latest_employees_paid', 0))} employees"
            )

        with col4:
            ot_hours = kpi_row.get('total_ot_hours_30d', 0)
            st.metric(
                "OT Hours (30d)",
                f"{ot_hours:,.0f}",
                delta="overtime" if ot_hours > 0 else None,
                delta_color="off"
            )

        st.divider()

        # Trend Charts Row
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Attendance Trend (30 Days)")
            try:
                attendance_trend = run_query("""
                    SELECT
                        attendance_date,
                        SUM(present_count) as present,
                        SUM(absent_count) as absent,
                        ROUND(100.0 * SUM(present_count) / NULLIF(SUM(total_records), 0), 1) as rate
                    FROM metrics.attendance_daily_metrics
                    WHERE attendance_date >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY attendance_date
                    ORDER BY attendance_date
                """)
                if len(attendance_trend) > 0:
                    fig = px.line(
                        attendance_trend,
                        x='attendance_date',
                        y='rate',
                        title=None,
                        labels={'attendance_date': 'Date', 'rate': 'Attendance Rate (%)'}
                    )
                    fig.update_layout(height=300)
                    fig.add_hline(y=90, line_dash="dash", line_color="green", annotation_text="Target 90%")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No attendance data available for the last 30 days")
            except Exception as e:
                st.warning(f"Could not load attendance trend: {e}")

        with col2:
            st.subheader("Payroll by Period")
            try:
                payroll_trend = run_query("""
                    SELECT
                        pay_period,
                        SUM(total_gross_pay) as gross_pay,
                        SUM(total_employer_cost) as employer_cost,
                        SUM(employees_paid) as employees
                    FROM metrics.payroll_period_metrics
                    WHERE pay_period >= STRFTIME(CURRENT_DATE - INTERVAL '6 months', '%Y-%m')
                    GROUP BY pay_period
                    ORDER BY pay_period
                """)
                if len(payroll_trend) > 0:
                    fig = px.bar(
                        payroll_trend,
                        x='pay_period',
                        y='gross_pay',
                        title=None,
                        labels={'pay_period': 'Period', 'gross_pay': 'Gross Pay ($)'}
                    )
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No payroll data available for the last 6 months")
            except Exception as e:
                st.warning(f"Could not load payroll trend: {e}")

        st.divider()

        # Alerts Section
        st.subheader("Alerts & Issues")
        try:
            alerts = run_query("""
                SELECT alert_type, priority, employee_name, description, detail
                FROM metrics.executive_alerts
                ORDER BY
                    CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
                    alert_type
                LIMIT 20
            """)

            if len(alerts) > 0:
                # Group by priority
                high_alerts = alerts[alerts['priority'] == 'High']
                medium_alerts = alerts[alerts['priority'] == 'Medium']
                low_alerts = alerts[alerts['priority'] == 'Low']

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.markdown("**🔴 High Priority**")
                    if len(high_alerts) > 0:
                        for _, alert in high_alerts.iterrows():
                            st.warning(f"**{alert['employee_name']}**: {alert['detail']}")
                    else:
                        st.success("No high priority alerts")

                with col2:
                    st.markdown("**🟡 Medium Priority**")
                    if len(medium_alerts) > 0:
                        for _, alert in medium_alerts.head(5).iterrows():
                            st.info(f"**{alert['employee_name']}**: {alert['detail']}")
                    else:
                        st.success("No medium priority alerts")

                with col3:
                    st.markdown("**🟢 Low Priority**")
                    if len(low_alerts) > 0:
                        for _, alert in low_alerts.head(5).iterrows():
                            st.caption(f"**{alert['employee_name']}**: {alert['detail']}")
                    else:
                        st.success("No low priority alerts")
            else:
                st.success("No alerts at this time")
        except Exception as e:
            st.warning(f"Could not load alerts: {e}")

    else:
        # Fallback to basic overview if KPIs view not available
        st.info("Executive KPIs view not available. Showing basic metrics.")

        col1, col2, col3, col4 = st.columns(4)

        employee_count = run_query("SELECT COUNT(DISTINCT employee_id) as cnt FROM staging.stg_payroll")['cnt'][0]
        payroll_records = run_query("SELECT COUNT(*) as cnt FROM raw.crmc_payrollfile")['cnt'][0]
        attendance_records = run_query("SELECT COUNT(*) as cnt FROM staging.stg_attendance")['cnt'][0]
        activity_records = run_query("SELECT COUNT(*) as cnt FROM raw.activity_log")['cnt'][0]

        col1.metric("Employees", f"{employee_count:,}")
        col2.metric("Payroll Records", f"{payroll_records:,}")
        col3.metric("Attendance Records", f"{attendance_records:,}")
        col4.metric("Activity Logs", f"{activity_records:,}")

# Workforce Demographics Page
elif page == "Workforce Demographics":
    st.title("Workforce Demographics")

    try:
        # Summary metrics
        summary = run_query("""
            SELECT
                COUNT(DISTINCT employee_int_id) as total_employees,
                COUNT(DISTINCT CASE WHEN employee_status = 'Active' THEN employee_int_id END) as active,
                COUNT(DISTINCT department_name) as departments,
                ROUND(AVG(tenure_years), 1) as avg_tenure
            FROM business.workforce_demographics
        """)

        if len(summary) > 0:
            s = summary.iloc[0]
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Employees", f"{int(s['total_employees']):,}")
            col2.metric("Active", f"{int(s['active']):,}")
            col3.metric("Departments", f"{int(s['departments']):,}")
            col4.metric("Avg Tenure (Years)", f"{s['avg_tenure']:.1f}")

        st.divider()

        # Department breakdown
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Headcount by Department")
            dept_data = run_query("""
                SELECT department_name, COUNT(*) as count
                FROM business.workforce_demographics
                WHERE employee_status = 'Active'
                GROUP BY department_name
                ORDER BY count DESC
                LIMIT 15
            """)
            if len(dept_data) > 0:
                fig = px.bar(dept_data, x='count', y='department_name', orientation='h',
                            labels={'count': 'Employees', 'department_name': 'Department'})
                fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Tenure Distribution")
            tenure_data = run_query("""
                SELECT tenure_band, COUNT(*) as count
                FROM business.workforce_demographics
                WHERE employee_status = 'Active'
                GROUP BY tenure_band
                ORDER BY
                    CASE tenure_band
                        WHEN '< 1 Year' THEN 1
                        WHEN '1-2 Years' THEN 2
                        WHEN '3-4 Years' THEN 3
                        WHEN '5-9 Years' THEN 4
                        WHEN '10-19 Years' THEN 5
                        WHEN '20+ Years' THEN 6
                        ELSE 7
                    END
            """)
            if len(tenure_data) > 0:
                fig = px.pie(tenure_data, values='count', names='tenure_band', hole=0.4)
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

        # More demographics
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Employee Types")
            type_data = run_query("""
                SELECT employee_type, COUNT(*) as count
                FROM business.workforce_demographics
                WHERE employee_status = 'Active'
                GROUP BY employee_type
                ORDER BY count DESC
            """)
            if len(type_data) > 0:
                fig = px.pie(type_data, values='count', names='employee_type')
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Manager Span of Control")
            span_data = run_query("""
                SELECT manager_span, COUNT(*) as count
                FROM business.workforce_demographics
                WHERE employee_status = 'Active'
                GROUP BY manager_span
                ORDER BY count DESC
            """)
            if len(span_data) > 0:
                fig = px.bar(span_data, x='manager_span', y='count',
                            labels={'count': 'Employees', 'manager_span': 'Team Size'})
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Detailed table
        st.subheader("Employee Details")

        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            dept_filter = st.selectbox("Department", ["All"] + list(run_query(
                "SELECT DISTINCT department_name FROM business.workforce_demographics WHERE department_name IS NOT NULL ORDER BY department_name"
            )['department_name']))
        with col2:
            status_filter = st.selectbox("Status", ["All", "Active", "Terminated"])
        with col3:
            tenure_filter = st.selectbox("Tenure", ["All", "< 1 Year", "1-2 Years", "3-4 Years", "5-9 Years", "10-19 Years", "20+ Years"])

        query = """
            SELECT employee_id, full_name, department_name, job_code_name,
                   employee_type, employee_status, tenure_band, tenure_years,
                   direct_reports, hourly_rate
            FROM business.workforce_demographics
            WHERE 1=1
        """
        params = []

        if dept_filter != "All":
            query += " AND department_name = ?"
            params.append(dept_filter)
        if status_filter != "All":
            query += " AND employee_status = ?"
            params.append(status_filter)
        if tenure_filter != "All":
            query += " AND tenure_band = ?"
            params.append(tenure_filter)

        query += " ORDER BY department_name, full_name LIMIT 500"

        if params:
            detail_df = run_query_safe(query, params)
        else:
            detail_df = run_query(query)

        st.dataframe(detail_df, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(detail_df)} employees")

    except Exception as e:
        st.error(f"Could not load workforce demographics: {e}")
        st.info("Please run `python init_semantic_layer.py` to create the required views.")

# Detailed Attendance Page
elif page == "Detailed Attendance":
    st.title("Detailed Attendance")

    try:
        # Date range filter
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=pd.Timestamp.now() - pd.Timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=pd.Timestamp.now())

        # Daily summary metrics
        daily_metrics = run_query(f"""
            SELECT
                SUM(total_records) as total_records,
                SUM(present_count) as present,
                SUM(absent_count) as absent,
                SUM(late_count) as late,
                ROUND(SUM(ot_hours), 1) as ot_hours,
                ROUND(100.0 * SUM(present_count) / NULLIF(SUM(total_records), 0), 1) as attendance_rate
            FROM metrics.attendance_daily_metrics
            WHERE attendance_date BETWEEN '{start_date}' AND '{end_date}'
        """)

        if len(daily_metrics) > 0:
            m = daily_metrics.iloc[0]
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Total Records", f"{int(m['total_records'] or 0):,}")
            col2.metric("Present", f"{int(m['present'] or 0):,}")
            col3.metric("Absent", f"{int(m['absent'] or 0):,}")
            col4.metric("Late Arrivals", f"{int(m['late'] or 0):,}")
            col5.metric("Attendance Rate", f"{m['attendance_rate'] or 0:.1f}%")

        st.divider()

        # Charts
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Daily Attendance Trend")
            trend = run_query(f"""
                SELECT attendance_date, SUM(present_count) as present, SUM(absent_count) as absent
                FROM metrics.attendance_daily_metrics
                WHERE attendance_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY attendance_date
                ORDER BY attendance_date
            """)
            if len(trend) > 0:
                fig = px.line(trend, x='attendance_date', y=['present', 'absent'],
                            labels={'value': 'Count', 'attendance_date': 'Date', 'variable': 'Status'})
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Attendance by Department")
            dept_att = run_query(f"""
                SELECT department_name,
                       SUM(present_count) as present,
                       SUM(absent_count) as absent,
                       ROUND(100.0 * SUM(present_count) / NULLIF(SUM(total_records), 0), 1) as rate
                FROM metrics.attendance_daily_metrics
                WHERE attendance_date BETWEEN '{start_date}' AND '{end_date}'
                  AND department_name IS NOT NULL
                GROUP BY department_name
                ORDER BY rate DESC
                LIMIT 10
            """)
            if len(dept_att) > 0:
                fig = px.bar(dept_att, x='rate', y='department_name', orientation='h',
                            labels={'rate': 'Attendance Rate (%)', 'department_name': 'Department'},
                            color='rate', color_continuous_scale='RdYlGn')
                fig.update_layout(height=300, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Detailed records
        st.subheader("Attendance Records")

        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            dept_filter = st.selectbox("Filter by Department", ["All"] + list(run_query(
                "SELECT DISTINCT department_name FROM business.daily_attendance_detail WHERE department_name IS NOT NULL ORDER BY department_name"
            )['department_name']))
        with col2:
            status_filter = st.selectbox("Filter by Status", ["All", "Present", "Absent", "Leave", "Half Day"])
        with col3:
            late_filter = st.checkbox("Show Late Only")

        query = f"""
            SELECT attendance_date, employee_name, department_name, attendance_status,
                   is_late, late_by_mins, total_hours, ot_hours
            FROM business.daily_attendance_detail
            WHERE attendance_date BETWEEN '{start_date}' AND '{end_date}'
        """

        if dept_filter != "All":
            query += f" AND department_name = '{dept_filter}'"
        if status_filter != "All":
            query += f" AND attendance_status = '{status_filter}'"
        if late_filter:
            query += " AND is_late = TRUE"

        query += " ORDER BY attendance_date DESC, employee_name LIMIT 500"

        records = run_query(query)
        st.dataframe(records, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(records)} records")

    except Exception as e:
        st.error(f"Could not load attendance data: {e}")
        st.info("Please run `python init_semantic_layer.py` to create the required views.")

# Payroll Analytics Page
elif page == "Payroll Analytics":
    st.title("Payroll Analytics")

    try:
        # Summary metrics
        summary = run_query("""
            SELECT
                COUNT(DISTINCT pay_period) as periods,
                SUM(total_gross_pay) as total_gross,
                SUM(total_employer_cost) as total_cost,
                AVG(avg_gross_pay) as avg_pay,
                SUM(employees_paid) as total_paid
            FROM metrics.payroll_period_metrics
            WHERE pay_year = EXTRACT(YEAR FROM CURRENT_DATE)
        """)

        if len(summary) > 0:
            s = summary.iloc[0]
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Pay Periods (YTD)", f"{int(s['periods'] or 0):,}")
            col2.metric("Total Gross Pay (YTD)", f"${s['total_gross'] or 0:,.0f}")
            col3.metric("Total Employer Cost (YTD)", f"${s['total_cost'] or 0:,.0f}")
            col4.metric("Avg Gross Pay", f"${s['avg_pay'] or 0:,.0f}")

        st.divider()

        # Charts
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Payroll Trend by Period")
            trend = run_query("""
                SELECT pay_period,
                       SUM(total_gross_pay) as gross_pay,
                       SUM(total_employer_cost) as employer_cost
                FROM metrics.payroll_period_metrics
                GROUP BY pay_period
                ORDER BY pay_period DESC
                LIMIT 12
            """)
            if len(trend) > 0:
                trend = trend.sort_values('pay_period')
                fig = px.bar(trend, x='pay_period', y=['gross_pay', 'employer_cost'],
                            barmode='group',
                            labels={'value': 'Amount ($)', 'pay_period': 'Period', 'variable': 'Type'})
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Payroll by Department (Latest Period)")
            dept_pay = run_query("""
                SELECT department_name, total_gross_pay, employees_paid
                FROM metrics.payroll_period_metrics
                WHERE pay_period = (SELECT MAX(pay_period) FROM metrics.payroll_period_metrics)
                  AND department_name IS NOT NULL
                ORDER BY total_gross_pay DESC
                LIMIT 10
            """)
            if len(dept_pay) > 0:
                fig = px.bar(dept_pay, x='total_gross_pay', y='department_name', orientation='h',
                            labels={'total_gross_pay': 'Gross Pay ($)', 'department_name': 'Department'})
                fig.update_layout(height=350, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)

        # Earnings breakdown
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top Earners (Latest Period)")
            top_earners = run_query("""
                SELECT employee_name, department_name, gross_pay, total_hours, effective_hourly_rate
                FROM business.payroll_analytics
                WHERE pay_period = (SELECT MAX(pay_period) FROM business.payroll_analytics)
                ORDER BY gross_pay DESC
                LIMIT 10
            """)
            if len(top_earners) > 0:
                st.dataframe(top_earners, use_container_width=True, hide_index=True,
                            column_config={
                                "gross_pay": st.column_config.NumberColumn("Gross Pay", format="$%.2f"),
                                "effective_hourly_rate": st.column_config.NumberColumn("Eff. Rate", format="$%.2f")
                            })

        with col2:
            st.subheader("Deductions & Taxes")
            tax_summary = run_query("""
                SELECT pay_period,
                       SUM(total_deductions) as deductions,
                       SUM(total_employee_taxes) as employee_taxes,
                       SUM(total_employer_taxes) as employer_taxes
                FROM metrics.payroll_period_metrics
                GROUP BY pay_period
                ORDER BY pay_period DESC
                LIMIT 6
            """)
            if len(tax_summary) > 0:
                tax_summary = tax_summary.sort_values('pay_period')
                fig = px.line(tax_summary, x='pay_period', y=['deductions', 'employee_taxes', 'employer_taxes'],
                             labels={'value': 'Amount ($)', 'pay_period': 'Period', 'variable': 'Type'})
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Detailed payroll records
        st.subheader("Payroll Records")

        # Filters
        col1, col2 = st.columns(2)
        with col1:
            period_options = run_query("SELECT DISTINCT pay_period FROM business.payroll_analytics ORDER BY pay_period DESC LIMIT 12")
            period_filter = st.selectbox("Pay Period", ["All"] + list(period_options['pay_period']))
        with col2:
            dept_filter = st.selectbox("Department", ["All"] + list(run_query(
                "SELECT DISTINCT department_name FROM business.payroll_analytics WHERE department_name IS NOT NULL ORDER BY department_name"
            )['department_name']))

        query = """
            SELECT employee_name, department_name, pay_period, gross_pay, net_pay,
                   deductions, total_hours, effective_hourly_rate
            FROM business.payroll_analytics
            WHERE 1=1
        """
        params = []

        if period_filter != "All":
            query += " AND pay_period = ?"
            params.append(period_filter)
        if dept_filter != "All":
            query += " AND department_name = ?"
            params.append(dept_filter)

        query += " ORDER BY gross_pay DESC LIMIT 500"

        if params:
            records = run_query_safe(query, params)
        else:
            records = run_query(query)

        st.dataframe(records, use_container_width=True, hide_index=True,
                    column_config={
                        "gross_pay": st.column_config.NumberColumn("Gross Pay", format="$%.2f"),
                        "net_pay": st.column_config.NumberColumn("Net Pay", format="$%.2f"),
                        "deductions": st.column_config.NumberColumn("Deductions", format="$%.2f"),
                        "effective_hourly_rate": st.column_config.NumberColumn("Eff. Rate", format="$%.2f")
                    })
        st.caption(f"Showing {len(records)} records")

    except Exception as e:
        st.error(f"Could not load payroll data: {e}")
        st.info("Please run `python init_semantic_layer.py` to create the required views.")

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
st.sidebar.caption("HRMS Semantic Layer v2.0")
st.sidebar.caption("Data: DuckDB + SQL Server")
