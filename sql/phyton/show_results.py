import matplotlib.pyplot as plt
import seaborn as sns
from queries import by_city, by_department, by_age, salary_by_city, hired_and_terminated

def plot_by_city():
    df = by_city()
    if df is not None:
        plt.figure(figsize=(12, 6))
        ax = sns.barplot(x='active_employees', y='city', data=df, palette='Blues_d')

        # Agregar valores al final de cada barra
        for i in ax.containers:
            ax.bar_label(i, fmt='%.0f', label_type='edge', padding=3)

        plt.title('Top 10 Active Employees by City')
        plt.xlabel('Active Employees')
        plt.ylabel('Departament')
        plt.tight_layout()
        plt.show()

def plot_by_department():
    df = by_department()
    if df is not None:
        plt.figure(figsize=(12, 6))
        sns.barplot(x='active_employees', y='department', data=df, palette='Purples_d')
        plt.title('Active Employees by Department')
        plt.xlabel('Active Employees')
        plt.ylabel('Departament')
        plt.tight_layout()
        plt.show()

def plot_by_age():
    df = by_age()
    if df is not None:
        plt.figure(figsize=(12, 6))
        sns.lineplot(x='age', y='turnover_rate', data=df, marker='o')
        plt.title('Turnover Rate by Age')
        plt.xlabel('Age')
        plt.ylabel('Turnover Rate')
        plt.tight_layout()
        plt.show()

def plot_salary_by_city():
    df = salary_by_city()
    if df is not None:
        plt.figure(figsize=(12, 6))
        sns.barplot(x='total_salary', y='city', data=df, palette='Greens_d')
        plt.title('Top 10 Total Salary by City (Monthly)')
        plt.xlabel('Total Salary')
        plt.ylabel('City')
        plt.tight_layout()
        plt.show()

def plot_hired_and_terminated():
    df = hired_and_terminated()
    if df is not None:
        plt.figure(figsize=(12, 6))
        plt.plot(df['year'], df['hired_count'], label='Hired', marker='o')
        plt.plot(df['year'], df['terminated_count'], label='Terminated', marker='o')
        plt.title('Hired and Terminated Employees by Year')
        plt.xlabel('Year')
        plt.ylabel('Count')
        plt.legend()
        plt.tight_layout()
        plt.show()

def main():
    print("=== Active Employees by City ===")
    plot_by_city()
    print("=== Active Employees by Department ===")
    plot_by_department()
    print("=== Turnover Rate by Age ===")
    plot_by_age()
    print("=== Total Salary by City (Monthly) ===")
    plot_salary_by_city()
    print("=== Hired and Terminated Employees by Year ===")
    plot_hired_and_terminated()

if __name__ == "__main__":
    main()