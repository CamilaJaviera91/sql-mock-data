import matplotlib.pyplot as plt
import seaborn as sns
from queries import by_city, by_department, by_age, salary_by_city, salary_by_department, hired_and_terminated

def plot_by_city():
    df = by_city()
    if df is not None:
        plt.figure(figsize=(12, 6))
        ax = sns.barplot(x='active_employees', y='city', data=df, palette='Blues_d')

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
        ax = sns.barplot(x='active_employees', y='department', data=df, palette='Purples_d')

        for i in ax.containers:
            ax.bar_label(i, fmt='%.0f', label_type='edge', padding=3)

        plt.title('Active Employees by Department')
        plt.xlabel('Active Employees')
        plt.ylabel('Departament')
        plt.tight_layout()
        plt.show()

def plot_by_age():
    df = by_age()
    if df is not None and not df.empty:
        plt.figure(figsize=(12, 6))
        ax = sns.lineplot(x='age', y='turnover_rate', data=df, marker='o', linewidth=2, color='teal')

        for x, y in zip(df['age'], df['turnover_rate']):
            ax.text(x, float(y) + 0.01, f'{float(y):.2f}', ha='center', va='bottom', fontsize=9)

        ax.set_title('Turnover Rate by Age')
        ax.set_xlabel('Age')
        ax.set_ylabel('Turnover Rate')
        ax.set_ylim(0, float(df['turnover_rate'].max()) + 0.1)
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.show()

def plot_salary_by_city():
    df = salary_by_city()
    if df is not None:
        plt.figure(figsize=(12, 6))
        ax = sns.barplot(x='total_salary', y='city', data=df, palette='Greens_d')

        for i in ax.containers:
            ax.bar_label(i, fmt='%.0f', label_type='edge', padding=3)

        plt.title('Top 10 Total Salary by City (Monthly)')
        plt.xlabel('Total Salary')
        plt.ylabel('City')
        plt.tight_layout()
        plt.show()

def plot_salary_by_department():
    df = salary_by_department()
    if df is not None:
        plt.figure(figsize=(12, 6))
        ax = sns.barplot(x='total_salary', y='department', data=df, palette='Greens_d')

        for i in ax.containers:
            ax.bar_label(i, fmt='%.0f', label_type='edge', padding=3)

        plt.title('Top 10 Total Salary by Department (Monthly)')
        plt.xlabel('Total Salary')
        plt.ylabel('Department')
        plt.tight_layout()
        plt.show()

def plot_hired_and_terminated():
    df = hired_and_terminated()
    if df is not None and not df.empty:
        plt.figure(figsize=(12, 6))

        plt.plot(df['year'], df['hired_count'], label='Hired', marker='o', color='green')
        plt.plot(df['year'], df['terminated_count'], label='Terminated', marker='o', color='red')

        for x, y in zip(df['year'], df['hired_count']):
            plt.text(x, y + 1, str(int(y)), ha='center', va='bottom', fontsize=9, color='green')

        for x, y in zip(df['year'], df['terminated_count']):
            plt.text(x, y + 1, str(int(y)), ha='center', va='bottom', fontsize=9, color='red')

        plt.title('Hired and Terminated Employees by Year')
        plt.xlabel('Year')
        plt.ylabel('Count')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
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
    print("=== Total Salary by Department (Monthly) ===")
    plot_salary_by_department()
    print("=== Hired and Terminated Employees by Year ===")
    plot_hired_and_terminated()

if __name__ == "__main__":
    main()