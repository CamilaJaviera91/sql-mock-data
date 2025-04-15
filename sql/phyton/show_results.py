import matplotlib.pyplot as plt
import seaborn as sns
from queries import by_city, by_department, by_age, hired_and_terminated

def plot_by_city():
    df = by_city()
    if df is not None:
        plt.figure(figsize=(12, 6))
        sns.barplot(x='active_employees', y='city', data=df, palette='Blues_d')
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
    df = by_department()
    if df is not None:
        plt.figure(figsize=(12, 6))
        sns.barplot(x='active_employees', y='department', data=df, palette='Purples_d')
        plt.title('Active Employees by Department')
        plt.xlabel('Active Employees')
        plt.ylabel('Departament')
        plt.tight_layout()
        plt.show()

def main():
    print("=== Active Employees by City ===")
    plot_by_city()
    print("=== Active Employees by Department ===")
    plot_by_department()
    print("=== Active Employees by Age ===")
    plot_by_age()

if __name__ == "__main__":
    main()