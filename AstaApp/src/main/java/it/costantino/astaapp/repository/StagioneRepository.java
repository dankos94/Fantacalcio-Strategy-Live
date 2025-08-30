package it.costantino.astaapp.repository;

import it.costantino.astaapp.domain.Stagione;
import org.springframework.data.jpa.repository.*;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA repository for the Stagione entity.
 */
@SuppressWarnings("unused")
@Repository
public interface StagioneRepository extends JpaRepository<Stagione, Long> {}
