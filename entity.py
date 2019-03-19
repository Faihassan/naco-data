import pandas as pd

pd.set_option("display.max_columns", 500)

# get data from the excel file
cd = pd.read_excel("naco.xlsx", "Company_Details")
gd = pd.read_excel("naco.xlsx", "Group_Details")

# Rename the columns to match the Excel importer
cd.rename(
    columns={
        "Company_Name": "Entity__OperatingName",
        "Company_LegalName": "Entity__LegalName",
        "Company_URL": "Entity__Website",
    },
    inplace=True,
)
gd.rename(
    columns={"Group_Name": "Entity__OperatingName", "Group_URL": "Entity__Website"},
    inplace=True,
)

# Drop the columns not needed for the entity table
cdc = cd.drop(
    [
        "Company_Email",
        "Company_PhoneNumber",
        "Company_Address1",
        "Company_Address2",
        "Company_City",
        "Company_Province",
        "Company_PostCode",
    ],
    axis=1,
)
gdc = gd.drop(
    [
        "Group_Address1",
        "Group_Address2",
        "Group_City",
        "Group_Province",
        "Group_PostCode",
        "Group_Region",
    ],
    axis=1,
)

# Merge the two sheets together to create one entity sheet
frames = [cdc, gdc]
entities = pd.concat(frames)

# Fill the missing Legal name with Operating Name
entities.Entity__LegalName = entities.Entity__LegalName.fillna(
    value=entities.Entity__OperatingName
)

# Add the Entities ID
entities["test"] = entities.index
entities["Entities__ID"] = "NEW_ENTITY" + entities["test"].astype(str)
entities = entities.drop(["test"], axis=1)

# Import Status table
cs = pd.read_excel("naco.xlsx", "Company_Status_2016")

# Rename the Name
cs.rename(columns={"Company_Name": "Entity__OperatingName"}, inplace=True)

# Join the tables
cst = entities.join(cs.set_index("Entity__OperatingName"), on="Entity__OperatingName")

# Drop the Age Column
e_status = cst.drop(["Age"], axis=1)

# Loading Group Characteristics
group_ch = pd.read_excel("naco.xlsx", "Group_Characteristics")

# Sort Values
group_ch = group_ch.sort_values("Group_SubmissionYear")

# Only Choose the data for 2018 submission to avoid multiple records
group_ch = group_ch[group_ch.Group_SubmissionYear == 2018]
group_ch.rename(columns={"Group_Name": "Entity__OperatingName"}, inplace=True)


# Join group characteristics table with entities
e_group = e_status.join(
    group_ch.set_index("Entity__OperatingName"), on=("Entity__OperatingName")
)

entity_sheet = e_group[
    [
        "Entities__ID",
        "Entity__LegalName",
        "Entity__OperatingName",
        "Entity__Website",
        "Still_In_Operation",
        "Employee_Count",
        "Group_Age",
        "Group_Fulltime",
        "Group_Parttime",
        "Group_Volunteer",
        "Group_Meetings_Number",
        "Group_Chapters_Number",
        "Group_Members_Number",
        "Group_IdentifyMale",
        "Group_IdentifyFemale",
        "Group_MembersInvested_Number",
        "Group_ApplicationsPitched_Num",
        "Group_PitchesDueDiligenced_Num",
        "Group_NewInvestments_Num",
        "Group_FollowonInvestments_Num",
        "Group_TotalInvestments_Num",
        "Group_NewInvestments_Dollar",
        "Group_FollowonInvestments_Dollar",
        "Group_TotalInvestments_Dollar",
    ]
]
entity_sheet.rename(
    columns={
        "Still_In_Operation": "Entity__OperatingStatus",
        "Employee_Count": "Entity__NumberOfEmployees",
        "Group_Age": "Entity__YearFounded",
        "Group_Fulltime": "Entity__NumberOfFullTimeEmployees",
        "Group_Parttime": "Entity__NumberOfPartTimeEmployees",
        "Group_Volunteer": "Entity__NumberOfVolunteers",
        "Group_Meetings_Number": "Entity__NumberOfInvestmentMeetings",
        "Group_Chapters_Number": "Entity__NumberOfChapters",
        "Group_Members_Number": "Entity__NumberOfMembers",
        "Group_IdentifyMale": "Entity__NumberOfMembers__Male",
        "Group_IdentifyFemale": "Entity__NumberOfMembers__Female",
        "Group_MembersInvested_Number": "Entity__NumberOfMembers__Investing",
        "Group_ApplicationsPitched_Num": "Program__NumberOfSubmissions__Received",
        "Group_PitchesDueDiligenced_Num": "Program__NumberOfSubmissions__SelectedForDueDiligence",
        "Group_NewInvestments_Num": "Entity__InvestmentsMade__Count__New",
        "Group_FollowonInvestments_Num": "Entity__InvestmentsMade__Count__FollowOn",
        "Group_NewInvestments_Num": "Entity__InvestmentsMade__Count",
        "Group_NewInvestments_Dollar": "Entity__InvestmentsMade__TotalAmount__New",
        "Group_FollowonInvestments_Dollar": "Entity__InvestmentsMade__TotalAmount__FollowOn",
        "Group_TotalInvestments_Dollar": "Entity__InvestmentsMade__TotalAmount",
    },
    inplace=True,
)

print(entity_sheet)
export = entity_sheet.to_csv("entity.csv")