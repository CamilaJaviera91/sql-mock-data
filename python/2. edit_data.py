import pandas as pd
import os
import random

# Paths
input_folder = "data"  # Folder containing your CSV files
output_folder = "data_enriched"  # Where enriched files will be saved
names_file = "female_names.txt"  # File with female names (one per line)

os.makedirs(output_folder, exist_ok=True)

# 1. Load female names from file
with open(names_file, "r", encoding="utf-8") as file:
    female_names = [line.strip() for line in file.readlines()]

# 2. Dictionary of job titles per department
job_titles = {
    "Sales": ["Sales Representative", "Account Executive", "Sales Manager", "Territory Manager",
              "Inside Sales Associate", "Regional Sales Director", "Sales Operations Analyst",
              "Business Development Executive", "Key Account Manager", "Retail Sales Associate",
              "Sales Consultant", "Sales Engineer", "Lead Generation Specialist", "Channel Sales Manager",
              "Customer Success Manager", "Enterprise Sales Executive", "Field Sales Representative",
              "Sales Enablement Specialist", "Area Sales Manager", "Account Manager"],
    "IT": ["Software Engineer", "Backend Developer", "Frontend Developer", "Full Stack Developer",
           "DevOps Engineer", "IT Support Specialist", "Systems Administrator", "Cloud Engineer",
           "Data Engineer", "Database Administrator", "Network Administrator", "Security Analyst",
           "QA Engineer", "Solutions Architect", "Machine Learning Engineer",
           "Business Intelligence Analyst", "IT Project Manager", "Help Desk Technician",
           "Mobile App Developer", "IT Consultant"],
    "Human Resources": ["HR Manager", "HR Business Partner", "Recruiter", "Talent Acquisition Specialist",
                        "HR Generalist", "HR Coordinator", "HR Assistant", "Training and Development Specialist",
                        "Compensation and Benefits Analyst", "Employee Relations Specialist", "HR Data Analyst",
                        "Payroll Specialist", "Diversity and Inclusion Manager", "HR Director", "HR Compliance Officer",
                        "Organizational Development Specialist", "Onboarding Coordinator", "Labor Relations Specialist",
                        "People Operations Specialist", "Workforce Planning Analyst"],
    "Marketing": ["Marketing Specialist", "Content Marketing Manager", "Digital Marketing Analyst",
                  "SEO Specialist", "Social Media Manager", "Email Marketing Coordinator",
                  "Product Marketing Manager", "Marketing Coordinator", "Marketing Director",
                  "Performance Marketing Analyst", "Brand Manager", "Copywriter", "Growth Marketing Manager",
                  "Graphic Designer", "PPC Specialist", "Influencer Marketing Manager",
                  "Communications Specialist", "Marketing Operations Manager", "Event Marketing Coordinator",
                  "PR Manager"],
    "Finance": ["Financial Analyst", "Accountant", "Controller", "Finance Manager", "Budget Analyst",
                "Financial Planner", "Tax Analyst", "Internal Auditor", "Cost Analyst", "Treasury Analyst",
                "Accounts Payable Specialist", "Accounts Receivable Specialist", "Investment Analyst",
                "Risk Analyst", "Credit Analyst", "Bookkeeper", "Payroll Analyst", "Compliance Analyst",
                "Finance Director", "Revenue Analyst"],
    "Operations": ["Operations Coordinator", "Operations Manager", "Logistics Specialist", "Inventory Analyst",
                   "Procurement Officer", "Supply Chain Analyst", "Warehouse Manager",
                   "Process Improvement Analyst", "Facilities Manager", "Distribution Supervisor",
                   "Vendor Manager", "Production Planner", "Manufacturing Supervisor", "Fleet Coordinator",
                   "Scheduling Analyst", "Quality Assurance Coordinator", "Demand Planner",
                   "Purchasing Assistant", "Plant Manager", "Operations Analyst"]
}

# Function to infer gender based on first name
def get_gender(name):
    first_name = name.split()[0]
    return "Female" if first_name in female_names else "Male"

# Function to assign a job title based on department
def assign_job_title(dept):
    titles = job_titles.get(dept, ["Staff"])
    return random.choice(titles)

# Process each CSV file
for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):
        df = pd.read_csv(os.path.join(input_folder, filename))

        # Add status based on termination_date
        df["status"] = df["termination_date"].apply(lambda x: "Inactive" if pd.notnull(x) else "Active")

        # Add gender based on name
        df["gender"] = df["name"].apply(get_gender)

        # Add job title based on department
        df["job_title"] = df["department"].apply(assign_job_title)

        # Save the enriched CSV
        df.to_csv(os.path.join(output_folder, filename), index=False)
        print(f"✔ Processed: {filename}")