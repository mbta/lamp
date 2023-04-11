# Script for populating the database. You can run it as:
#
#     mix run priv/repo/seeds.exs
#
IO.puts("Seeding Metadata Using Poetry")

# get the config for our database from the application env
db_config = Application.get_env(:api, Api.Repo)

# run the seed metadata script in the python directory, clearing out all data
# before populating the metadata log table. set the environment variables so
# that it is adding metadata to the appropriate database
{output, result} =
  System.shell(
    "poetry run snapshot reset",
    cd: "../python_src",
    stderr_to_stdout: true,
    env: %{
      "BOOTSTRAPPED" => "1",
      "DB_HOST" => db_config[:hostname],
      "DB_PORT" => db_config[:port],
      "DB_NAME" => db_config[:database],
      "DB_USER" => db_config[:username],
      "DB_PASSWORD" => db_config[:password]
    }
  )

# if the result is non zero, log it and exit
if result != 0 do
  IO.puts(output)
  exit(result)
end
