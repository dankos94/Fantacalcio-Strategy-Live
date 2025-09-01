package it.costantino.astaapp.repository;

import it.costantino.astaapp.domain.Roster;
import org.springframework.data.jpa.repository.*;
import org.springframework.stereotype.Repository;
import java.util.Optional;

/**
 * Spring Data JPA repository for the Roster entity.
 */
@SuppressWarnings("unused")
@Repository
public interface RosterRepository extends JpaRepository<Roster, Long> {
    
    Optional<Roster> findBySquadraId(Long squadraId);
}
