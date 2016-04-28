void get_credentials(std::string &access_key_id,
					 std::string &access_key_secret)
{
	std::string home = getenv("HOME");
	FILE *file = fopen("/home/jason/.aws/credentials", "r");

	size_t n = 0;
	char *lptr = 0;

	while (true)
	{
		ssize_t nr = getline(&lptr, &n, file);

		if (nr <= 0)
			break;

		if (lptr[nr-1] == '\n')
		{
			lptr[nr-1] = '\0';
			nr--;
		}

		if (lptr[0] == '[')
			continue;

		char *sptr = 0;

		char *t1 = strtok_r(lptr, " ", &sptr);
		if (!t1) continue;

		char *t2 = strtok_r(NULL, " ", &sptr);
		if (!t2) continue;

		char *t3 = strtok_r(NULL, " ", &sptr);
		if (!t3) continue;

		if (strcmp(t1, "aws_access_key_id") == 0)
		{
			access_key_id = t3;
		}
		else if (strcmp(t1, "aws_secret_access_key") == 0)
		{
			access_key_secret = t3;
		}
	}

	fclose(file);
}
