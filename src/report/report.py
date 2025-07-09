import pandas as pd, matplotlib.pyplot as plt
from sqlalchemy.engine import create_engine
from fpdf import FPDF
import argparse, os, datetime as dt

engine = create_engine("hive://hiveserver:10000/default")

def fetch_kpis(run_month):
    query = f"SELECT * FROM kpi_agg_monthly WHERE month_ym='{run_month}'"
    return pd.read_sql(query, engine)

def make_charts(df, out_dir):
    # Spend by segment
    fig1 = df.groupby("segment")["spend_usd"].sum().plot(kind="barh", title="Spend by Segment")
    fig1.figure.tight_layout(); fig1.figure.savefig(f"{out_dir}/spend.png"); plt.close(fig1.figure)
    # Activation & churn
    fig2 = df.plot(x="segment", y=["activation_rate","churn_rate"], kind="bar")
    fig2.figure.tight_layout(); fig2.figure.savefig(f"{out_dir}/rates.png"); plt.close(fig2.figure)

def build_pdf(df, out_dir, run_month):
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, f"KPI Digest – {run_month}", ln=True)
    pdf.set_font("Arial", size=10)
    pdf.ln(5)
    pdf.multi_cell(0, 5, f"""Highlights:
 • Total Spend: ${df['spend_usd'].sum():,.0f}
 • Activation Rate (avg): {df['activation_rate'].mean():.2%}
 • Churn Rate (avg): {df['churn_rate'].mean():.2%}
 • Best ROI treatment: {df.sort_values('roi_pct', ascending=False)['treatment'].iloc[0]}""")
    pdf.image(f"{out_dir}/spend.png", w=150)
    pdf.add_page()
    pdf.image(f"{out_dir}/rates.png", w=150)
    pdf.output(f"{out_dir}/kpi_digest_{run_month}.pdf")

if __name__ == "__main__":
    run_month = argparse.ArgumentParser().add_argument("--month").parse_args().month
    out_dir   = f"reports/{run_month}"
    os.makedirs(out_dir, exist_ok=True)
    df = fetch_kpis(run_month)
    make_charts(df, out_dir)
    build_pdf(df, out_dir, run_month)
