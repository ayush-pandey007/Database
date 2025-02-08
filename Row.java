public class Row {
  

        public int id;
        public String userName;
        public String email;

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getUserName() {
		return this.userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getEmail() {
		return this.email;
	}

	public void setEmail(String email) {
		this.email = email;
	}


       Row(int id,String userName,String email) {

           this.id = id;
           this.userName = userName;
           this.email = email;
       }
  
}
