import pandas as pd

pd.set_option("display.expand_frame_repr", False)
# get data from the excel file
deal = (
    pd.read_excel("naco.xlsx", "Deals")
    .drop(
        columns={
            "Deal_GovCoinvest_Dlr",
            "Deal_InvestorOutsideProvince_Dlr",
            "Deal_InvestorOutsideProvince_Num",
            "Deal_InvestedOtherAngelGrps_Dlr",
            "Deal_OtherInvestors",
            "Deal_InvestedOtherInvestors_Dlr",
            "Deal_GUID",
            "Group_NameAndSubmissionYear",
        }
    )
    .rename(
        columns={
            "Company_Name": "to_entity",
            "Group_Name": "from_entity",
            "Deal_FTECreated": "Deal__NumberOfFullTimeEmployeesCreated",
            "Deal_PriorRevenue": "Entity__TotalOperatingRevenue",
            "Deal_PremoneyValue": "Deal__PreMoneyValuation",
            "Deal_NewOrFollowon": "Transaction__FollowOnInvestment",
            "Deal_DollarInvested": "Transaction__Amount",
            "Deal_TotalInvested_Dlr": "Deal__Amount",
            "Deal_MemberInvestors_Num": "Transaction__NumberOfMembersInvested",
            "Deal_Date": "Transaction__Date",
        }
    )
)
deal = deal[
    [
        "Deal_DealRef",
        "to_entity",
        "from_entity",
        "Deal__NumberOfFullTimeEmployeesCreated",
        "Entity__TotalOperatingRevenue",
        "Deal__PreMoneyValuation",
        "Transaction__FollowOnInvestment",
        "Transaction__Amount",
        "Transaction__NumberOfMembersInvested",
        "Transaction__Date",
        "Deal__Amount",
    ]
]


exit = (
    pd.read_excel("naco.xlsx", "All_Exits")
    .drop(
        columns={
            "Exit_InvestmentRounds_Num",
            "Exit_FTE",
            "Group_NameAndSubmissionYear",
            "Exit_ExitRef",
        }
    )
    .rename(
        columns={
            "Company_Name": "to_entity",
            "Group_Name": "from_entity",
            "Exit_ExitType": "Transaction__Exit__Strategy",
            "Exit_ExitDate": "Transaction__Date",
            "Exit_OriginalInvestYear": "Transaction__Exit__YearOfFirstInvestment",
            "Exit_TotalInvestment_Dlr": "Transaction__Amount",
            "Exit_ROI": "Transaction__Exit__ROI",
        }
    )
)

deal_exit = pd.concat([deal, exit])


deal_stru = (
    pd.read_excel("naco.xlsx", "Deal_Structure")
    .drop(columns={"Deal_StructureRef"})
    .drop_duplicates(subset="Deal_DealRef", keep="first", inplace=False)
)

deal_exit_ex = deal_exit.join(deal_stru.set_index("Deal_DealRef"), on="Deal_DealRef")
deal_exit_ex = deal_exit_ex.drop(columns={"Deal_DealRef"})
deal_exit_ex = deal_exit_ex.fillna("")

deal_structure_map = pd.read_excel("naco.xlsx", "Deal_Structure_Map").fillna("")

deals = (
    deal_exit_ex.join(
        deal_structure_map.set_index("Deal_Structure"), on="Deal_Structure"
    )
    .fillna("")
    .drop(columns={"Deal_Structure"})
)
company_detail = pd.read_excel("naco.xlsx", "Company_Details")
group_detail = pd.read_excel("naco.xlsx", "Group_Details")

# Rename the columns to match the Excel importer
company_detail = company_detail[["Company_Name", "Company_LegalName", "Company_URL"]]
company_detail.rename(
    columns={
        "Company_Name": "Entity__OperatingName",
        "Company_LegalName": "Entity__LegalName",
        "Company_URL": "Entity__Website",
    },
    inplace=True,
)
group_detail = group_detail[["Group_Name", "Group_URL"]]
group_detail.rename(
    columns={"Group_Name": "Entity__OperatingName", "Group_URL": "Entity__Website"},
    inplace=True,
)

# Merge the two sheets together to create one entity sheet
entities = pd.concat([company_detail, group_detail])
entities["Entity__LegalName"] = entities["Entity__LegalName"].fillna(
    entities.Entity__OperatingName
)


# Import Status table
company_status = (
    pd.read_excel("naco.xlsx", "Company_Status_2016")
    .rename(columns={"Company_Name": "Entity__OperatingName"})
    .merge(entities, on="Entity__OperatingName", how="outer")
    .drop(["Age"], axis=1)
)

# Loading Group Characteristics
group_char = pd.read_excel("naco.xlsx", "Group_Characteristics").rename(
    columns={"Group_Name": "Entity__OperatingName"}
)

# Only Choose the data for 2018 submission to avoid multiple records
group_char = group_char[group_char.Group_SubmissionYear == 2018]

# Join group characteristics table with
entity_group_join = company_status.join(
    group_char.set_index("Entity__OperatingName"), on=("Entity__OperatingName")
)

entity_group_join = entity_group_join.rename(
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
        "Group_TotalInvestments_Num": "Entity__InvestmentsMade__Count",
        "Group_NewInvestments_Dollar": "Entity__InvestmentsMade__TotalAmount__New",
        "Group_FollowonInvestments_Dollar": "Entity__InvestmentsMade__TotalAmount__FollowOn",
        "Group_TotalInvestments_Dollar": "Entity__InvestmentsMade__TotalAmount",
    }
)

entity_sheet = entity_group_join[
    [
        "Entity__LegalName",
        "Entity__OperatingName",
        "Entity__Website",
        "Entity__OperatingStatus",
        "Entity__NumberOfEmployees",
        "Entity__YearFounded",
        "Entity__NumberOfFullTimeEmployees",
        "Entity__NumberOfPartTimeEmployees",
        "Entity__NumberOfVolunteers",
        "Entity__NumberOfInvestmentMeetings",
        "Entity__NumberOfChapters",
        "Entity__NumberOfMembers",
        "Entity__NumberOfMembers__Male",
        "Entity__NumberOfMembers__Female",
        "Entity__NumberOfMembers__Investing",
        "Program__NumberOfSubmissions__Received",
        "Program__NumberOfSubmissions__SelectedForDueDiligence",
        "Entity__InvestmentsMade__Count__New",
        "Entity__InvestmentsMade__Count__FollowOn",
        "Entity__InvestmentsMade__Count",
        "Entity__InvestmentsMade__TotalAmount__New",
        "Entity__InvestmentsMade__TotalAmount__FollowOn",
        "Entity__InvestmentsMade__TotalAmount",
    ]
]
company_sectors = (
    pd.read_excel("naco.xlsx", "Company_Sectors")
    .drop(columns={"Company_SectorRef", "Company_SectorsOther"})
    .drop_duplicates(subset="Company_Name", keep="first", inplace=False)
    .rename(
        columns={
            "Company_Name": "Entity__OperatingName",
            "Company_Sectors": "Entity__Sector",
        }
    )
)
entity_sheet = entity_sheet.join(
    company_sectors.set_index("Entity__OperatingName"), on=("Entity__OperatingName")
)
entity_sheet["index"] = entity_sheet.index
entity_sheet["Entities__ID"] = "NEW_ENTITY" + entity_sheet["index"].astype(str)
entity_sheet = entity_sheet.drop(["index"], axis=1)
entity_sheet = entity_sheet.drop_duplicates(
    subset="Entity__OperatingName", keep="first", inplace=False
)
entity_sheet["Entity__Sector"] = entity_sheet["Entity__Sector"].map(
    {
        "Clean Technologies: Alternative Energy": "Sectors::Energy::Energy",
        "Clean Technologies: Environmental Technology": "Sectors::Energy::Energy",
        "Clean Technologies: Services and Equipment": "Sectors::Energy::Energy",
        "Clean Technologies: Unspecified": "Sectors::Energy::Energy",
        "Clean Technologies: Water Technologies": "Sectors::Energy::Energy",
        "Energy: Infrastructure Technologies": "Sectors::Energy::Energy::EnergyEquipmentServices::OilGasEquipmentServices",
        "Energy: Oil and Gas": "Sectors::Energy::Energy::OilGasConsumableFuels",
        "Energy: Power and Utilities": "Sectors::Energy",
        "Energy: Unspecified": "Sectors::Energy::Energy",
        "ICT: Digital Media": "Sectors::InformationTechnology::SoftwareServices::InternetSoftwareServices::InternetSoftwareServices",
        "ICT: Mobile & Telecom": "Sectors::InformationTechnology::TechnologyHardwareEquipment::CommunicationsEquipment",
        "ICT: Software": "Sectors::InformationTechnology::SoftwareServices::InternetSoftwareServices::InternetSoftwareServices",
        "ICT: Unspecified": "Sectors::InformationTechnology",
        "ICT: Web-based Services": "Sectors::InformationTechnology::SoftwareServices::InternetSoftwareServices::InternetSoftwareServices",
        "Life Sciences: Biotech and Pharmaceuticals": "Sectors::HealthCare::PharmaceuticalsBiotechnologyLifeSciences",
        "Life Sciences: Healthcare and Services": "Sectors::HealthCare::HealthCareEquipmentServices::HealthCareProvidersServices::HealthCareServices",
        "Life Sciences: Medical Equipment & Technologies": "Sectors::HealthCare::HealthCareEquipmentServices",
        "Life Sciences: Unspecified": "Sectors::HealthCare",
        "Manufacturing: Automotive & Transportation": "Sectors::Industrials::Transportation::TransportationInfrastructure",
        "Manufacturing: Computers": "Sectors::InformationTechnology::TechnologyHardwareEquipment::ElectronicEquipmentInstrumentsComponents::ElectronicManufacturingServices",
        "Manufacturing: Electronics": "Sectors::Industrials::CapitalGoods::ElectricalEquipment::ElectricalComponentsEquipment",
        "Manufacturing: Industrial and Advanced Materials": "Sectors::Industrials::CapitalGoods::Machinery",
        "Manufacturing: Unspecified": "Sectors::Industrials",
        "No Sector Focus": "",
        "Other (Please specify)": "",
        "Other: Forestry and Agriculture": "Sectors::Materials::Materials::PaperForestProducts",
        "Other: Mining and Related": "Sectors::Materials::Materials::MetalsMining::DiversifiedMetalsMining",
        "Other: Nano-Technologies": "Sectors::InformationTechnology::TechnologyHardwareEquipment",
        "Other: Social Enterprises": "Sectors::InformationTechnology::SoftwareServices::InternetSoftwareServices",
        "Services: Financial Services": "Sectors::Financials",
        "Services: Food and Beverage": "Sectors::ConsumerDiscretionary::ConsumerServices::HotelsRestaurantsLeisure::Restaurants",
        "Services: Leisure and Tourism": "Sectors::ConsumerDiscretionary::ConsumerServices::HotelsRestaurantsLeisure::LeisureFacilities",
        "Services: Retail": "Sectors::ConsumerDiscretionary::Retailing",
        "Services: Risk and Security": "Sectors::Industrials::CommercialProfessionalServices::CommercialServicesSupplies::SecurityAlarmServices",
        "Services: Unspecified": "Sectors::ConsumerDiscretionary::ConsumerServices",
    }
)
entity_sheet["Entity__OperatingStatus"] = entity_sheet["Entity__OperatingStatus"].map(
    {
        "Yes": "Entity::OperatingStatus::Active",
        "No": "Entity::OperatingStatus::Inactive",
    }
)
entity_sheet = entity_sheet.fillna("")

entity_sheet_temp = entity_sheet[["Entity__OperatingName", "Entities__ID"]]

deals_to_entity = deals.join(
    entity_sheet_temp.set_index("Entity__OperatingName"), on=("to_entity"), how="left"
)
deals_to_entity = deals_to_entity.rename(
    columns={"Entities__ID": "Transaction__ToEntityID"}
)

deals_from_entity = deals_to_entity.join(
    entity_sheet_temp.set_index("Entity__OperatingName"), on="from_entity", how="left"
)
deals_from_entity = deals_from_entity.rename(
    columns={"Entities__ID": "Transaction__FromEntityID"}
)
deals_from_entity = deals_from_entity.join(
    entity_sheet_temp.set_index("Entity__OperatingName"), on="to_entity", how="left"
)
deals_from_entity = deals_from_entity.rename(columns={"Entities__ID": "Deal__EntityID"})
deal_sheet = deals_from_entity.drop(columns={"from_entity", "to_entity"})
deal_sheet["index"] = deal_sheet.index
deal_sheet["Deal__ID"] = "NEW_DEAL" + deal_sheet["index"].astype(str)
deal_sheet = deal_sheet.drop(columns={"index"})
deal_sheet["Transaction__FollowOnInvestment"] = deal_sheet[
    "Transaction__FollowOnInvestment"
].map({"Follow-On": "Yes", "New": "No"})
deal_sheet["Transaction__Exit__Strategy"] = deal_sheet[
    "Transaction__Exit__Strategy"
].map(
    {
        "Sale to / Merger with another company": "Strategy::MergerAndAcquisition",
        "Sale to new shareholders": "Strategy::Other",
        "Company ceased operations": "Strategy::Liquidation",
        "Sale to other existing shareholders": "Strategy::Other",
        "IPO": "Strategy::IPO",
        "Other": "Strategy::Other",
        "Unknown": "",
    }
)
deal_sheet = deal_sheet.fillna("")
# Deal Table with only deal concepts
deal_table = deal_sheet[
    [
        "Deal__ID",
        "Deal__EntityID",
        "Deal__Amount",
        "Deal__NumberOfFullTimeEmployeesCreated",
        "Deal__PreMoneyValuation",
    ]
]

# Transaction Table with only transaction Concepts
transaction_table = deal_sheet.drop(
    columns={
        "Deal__EntityID",
        "Deal__ID",
        "Deal__PreMoneyValuation",
        "Deal__NumberOfFullTimeEmployeesCreated",
        "Deal__Amount",
        "Entity__TotalOperatingRevenue",
    }
)
# transaction_table ['Transaction__Date'] = pd.to_datetime(transaction_table.Transaction__Date)

# excel_importer = pd.ExcelWriter("Consolidated_NACO.xlsx", engine="xlsxwriter")

# entity_sheet.to_excel(excel_importer, sheet_name="Entities", index=False)
# deal_table.to_excel(excel_importer, sheet_name="Deals", index=False)
# transaction_table.to_excel(excel_importer, sheet_name="Transactions", index=False)

# excel_importer.save()

excel_file = 'Consolidated_NACO.xlsx'
entities = pd.read_excel(excel_file, sheet_name='Entities')
deals = pd.read_excel(excel_file, sheet_name='Deals')
transactions = pd.read_excel(excel_file, sheet_name='Transactions')

investors = list(set(transactions['Transaction__FromEntityID']))
investors = sorted(investors)

for investor in investors:

    transaction_frame = transactions.loc[transactions['Transaction__FromEntityID'] == investor]
    investees = list(set(transaction_frame['Transaction__ToEntityID']))
    entity_list = investees
    entity_list.append(investor)
    deal_frame = deals.loc[deals['Deal__EntityID'].isin(investees)]
    entity_frame = entities.loc[entities['Entities__ID'].isin(entity_list)]

    name = entities.loc[entities['Entities__ID'] == investor, 'Entity__LegalName'].iloc[0]
    output = name + '.xlsx'
    writer = pd.ExcelWriter(output)
    transaction_frame.to_excel(writer, 'Transactions', index=False)
    entity_frame.to_excel(writer, 'Entities', index=False)
    deal_frame.to_excel(writer, 'Deals', index=False)
    writer.save()

print('Done')
