




public class Group_Key {
	String OutputJSONFilename;
	String AgencyCode;
	String AgencyName;
	String IsDivision;
	String BF_FORM_INSC_CD;
	
	public Group_Key(String my_output_json_filename, String my_agencyCode, String my_agencyName, String my_isDivision, String code)
	{
		OutputJSONFilename = my_output_json_filename;
		AgencyCode = my_agencyCode;
		AgencyName = my_agencyName;
		IsDivision = my_isDivision;
		BF_FORM_INSC_CD = code;
	}
}