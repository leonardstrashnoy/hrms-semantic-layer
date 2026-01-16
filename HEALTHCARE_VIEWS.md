# Healthcare-Specific Views

This semantic layer includes specialized views for hospital workforce management and compliance.

## Healthcare Business Views

### 1. `business.clinical_staff_summary`
Complete view of clinical workforce with healthcare-specific classifications.

**Key Features:**
- **Clinical Role Classification**: Automatically categorizes as RN, LPN, CNA, Physician, APP, Technician, etc.
- **Care Unit Type**: Classifies departments (ICU, ER, Surgery, L&D, Pediatrics, etc.)
- **Burnout Risk Scoring**: Flags staff based on overtime levels (High/Moderate/Low)
- **Overtime Tracking**: YTD overtime hours and percentage

**Common Queries:**
```sql
-- Find high-burnout risk nurses in ICU
SELECT full_name, department, ytd_overtime_hours, burnout_risk_level
FROM business.clinical_staff_summary
WHERE clinical_role = 'Registered Nurse'
  AND care_unit_type = 'ICU/Critical Care'
  AND burnout_risk_level IN ('High Risk', 'Moderate Risk')
ORDER BY ytd_overtime_hours DESC;

-- Active clinical staff by role
SELECT clinical_role, care_unit_type, COUNT(*) as staff_count
FROM business.clinical_staff_summary
WHERE employment_status = 'Active'
GROUP BY clinical_role, care_unit_type;
```

### 2. `business.staffing_by_shift`
Analyzes employee coverage by shift for 24/7 operations.

**Key Features:**
- **Shift Classification**: Day (6am-2pm), Evening (2pm-10pm), Night (10pm-6am)
- **Day Type**: Weekday vs Weekend
- **Overtime Flags**: Extended shifts (>12 hours), overtime hours
- **Pay Differential Eligibility**: Night, Evening, Weekend differentials

**Common Queries:**
```sql
-- Night shift staffing last week
SELECT
    attendance_date,
    department,
    COUNT(*) as staff_count,
    SUM(hours_worked) as total_hours,
    SUM(overtime_hours) as total_overtime
FROM business.staffing_by_shift
WHERE shift_type = 'Night Shift'
  AND attendance_date >= CURRENT_DATE - 7
GROUP BY attendance_date, department
ORDER BY attendance_date DESC;

-- Weekend coverage analysis
SELECT department, COUNT(DISTINCT employee_id) as unique_staff
FROM business.staffing_by_shift
WHERE day_type = 'Weekend'
  AND attendance_date >= CURRENT_DATE - 30
GROUP BY department;
```

## Healthcare Metrics Views

### 3. `metrics.shift_coverage_metrics`
Aggregated staffing metrics by shift, department, and time period.

**Key Metrics:**
- Staff count per shift type
- Average hours per shift
- Overtime percentage
- Extended shifts (>12 hours) count
- Pay differential eligibility

**Common Queries:**
```sql
-- Monthly shift coverage by department
SELECT
    year_month,
    department,
    shift_type,
    AVG(staff_count) as avg_staff,
    AVG(overtime_pct) as avg_overtime_pct
FROM metrics.shift_coverage_metrics
GROUP BY year_month, department, shift_type
ORDER BY year_month DESC, department;

-- Identify weeks with high overtime
SELECT
    week_start_date,
    shift_type,
    SUM(total_overtime_hours) as total_ot,
    AVG(overtime_pct) as avg_ot_pct
FROM metrics.shift_coverage_metrics
GROUP BY week_start_date, shift_type
HAVING AVG(overtime_pct) > 15
ORDER BY total_ot DESC;
```

### 4. `metrics.clinical_workforce_metrics`
Comprehensive workforce metrics for healthcare staff.

**Key Metrics:**
- Headcount by clinical role and care unit
- Average tenure and turnover indicators
- Burnout risk distribution
- Attendance rates
- Compensation analysis

**Common Queries:**
```sql
-- Burnout risk by department
SELECT
    department,
    clinical_role,
    staff_count,
    high_burnout_risk_count,
    moderate_burnout_risk_count,
    burnout_risk_pct
FROM metrics.clinical_workforce_metrics
WHERE employment_status = 'Active'
  AND burnout_risk_pct > 20
ORDER BY burnout_risk_pct DESC;

-- New hire integration tracking
SELECT
    clinical_role,
    care_unit_type,
    staff_count,
    new_hires_under_1_year,
    ROUND(new_hires_under_1_year::DECIMAL / staff_count * 100, 1) as new_hire_pct
FROM metrics.clinical_workforce_metrics
WHERE employment_status = 'Active'
ORDER BY new_hire_pct DESC;
```

### 5. `metrics.department_staffing_ratios`
Critical staffing ratios for patient care quality.

**Key Metrics:**
- RN, LPN, CNA counts by shift
- Minimum/maximum staffing levels
- Understaffed shift identification
- Staffing variability (scheduling consistency)

**Common Queries:**
```sql
-- ICU staffing compliance check
SELECT
    week_start_date,
    shift_type,
    avg_rn_count,
    min_rn_count,
    days_below_min_rn_icu
FROM metrics.department_staffing_ratios
WHERE department LIKE '%ICU%'
  AND week_start_date >= CURRENT_DATE - INTERVAL '4 weeks'
ORDER BY week_start_date DESC;

-- Identify consistently understaffed shifts
SELECT
    department,
    shift_type,
    avg_total_staff_count,
    min_total_staff_count,
    understaffed_days_pct
FROM metrics.department_staffing_ratios
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
  AND understaffed_days_pct > 10
ORDER BY understaffed_days_pct DESC;
```

## Healthcare Analytics Script

Run comprehensive healthcare workforce analytics:

```bash
python scripts/healthcare_analytics.py
```

**Reports Included:**
1. Clinical Staff Distribution by Role and Care Unit
2. Burnout Risk Analysis (High Overtime Staff)
3. Shift Coverage - Last 4 Weeks
4. Department Staffing Levels (Current Month)
5. Critical Care Units Workforce Summary
6. Shift Differential Pay Analysis
7. Potential Turnover Risk - New vs Experienced Staff

## Common Healthcare Analytics Use Cases

### Compliance & Safety
```sql
-- Ensure adequate RN coverage in critical care
SELECT * FROM metrics.department_staffing_ratios
WHERE care_unit_type = 'ICU/Critical Care'
  AND min_rn_count < 2;  -- Below safe minimum
```

### Financial Planning
```sql
-- Calculate shift differential costs
SELECT
    SUM(shifts_with_differential * avg_hours_per_shift) as differential_hours,
    shift_type,
    day_type
FROM metrics.shift_coverage_metrics
WHERE year_month = '2024-01'
GROUP BY shift_type, day_type;
```

### Retention & Burnout Prevention
```sql
-- Identify staff at risk of leaving
SELECT
    full_name,
    department,
    clinical_role,
    ytd_overtime_hours,
    burnout_risk_level,
    attendance_rate_pct
FROM business.clinical_staff_summary
WHERE burnout_risk_level = 'High Risk'
   OR attendance_rate_pct < 90
ORDER BY ytd_overtime_hours DESC;
```

### Scheduling Optimization
```sql
-- Find shifts with high variability (poor scheduling consistency)
SELECT
    department,
    shift_type,
    staff_count_stddev,
    min_total_staff_count,
    max_total_staff_count
FROM metrics.department_staffing_ratios
WHERE staff_count_stddev > 2  -- High variability
ORDER BY staff_count_stddev DESC;
```

## Regulatory & Joint Commission Readiness

These views help prepare for:

**Staffing Audits:**
- Minimum staffing ratios by unit
- RN-to-patient ratios (when patient data is added)
- Coverage gaps identification

**Labor Compliance:**
- Overtime tracking (FLSA compliance)
- Shift differential documentation
- Hours worked validation

**Quality Metrics:**
- Staff burnout indicators
- Turnover risk assessment
- Training and tenure tracking

## Future Enhancements

To add when data becomes available:

1. **Patient Census Integration**
   - RN-to-patient ratios
   - Acuity-adjusted staffing

2. **Certification Tracking**
   - License expiration alerts
   - Required training compliance
   - Specialty certifications

3. **Call/On-Call Scheduling**
   - On-call hours tracking
   - Call-back frequency
   - Emergency response times

4. **Skills Mix Analysis**
   - Critical skills inventory
   - Cross-training tracking
   - Float pool management

5. **Agency/Traveler Staff**
   - Contract vs FTE ratios
   - Cost comparison
   - Fill rate analysis
